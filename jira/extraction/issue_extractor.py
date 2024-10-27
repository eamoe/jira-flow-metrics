import logging
import requests_cache
from typing import Any, Dict, Generator, Iterator, List, Optional, Union

from jira.api.client import ApiClient

logger = logging.getLogger(__name__)


class JiraIssueExtractor:
    """Extracts issues and changelogs with logic specific to Jira issues."""
    def __init__(self, client: ApiClient) -> None:
        self.client = client

    def __get_status_categories(self) -> Dict[str, Any]:
        """
        Fetch all status categories from Jira.

        Returns:
            Dict[str, Any]: List of status categories.
        """
        return self.client.request('GET', '/rest/api/3/statuscategory')

    def __get_statuses(self) -> Dict[str, Any]:
        """
        Fetch all available statuses in Jira.

        Returns:
            Dict[str, Any]: List of statuses.
        """
        return self.client.request('GET', '/rest/api/3/status')

    def __get_project(self, project_key: str) -> Dict[str, Any]:
        """
        Fetch details of a specific project by key.

        Args:
            project_key (str): Key of the project to retrieve.

        Returns:
            Dict[str, Any]: Project details.
        """
        return self.client.request('GET', f'/rest/api/3/project/{project_key}')

    def __get_project_statuses(self, project_key: str) -> Dict[str, Any]:
        """
        Fetch all statuses for a given project.

        Args:
            project_key (str): Key of the project to retrieve statuses.

        Returns:
            Dict[str, Any]: List of statuses for the project.
        """
        return self.client.request('GET', f'/rest/api/3/project/{project_key}/statuses')

    def __search_issues(self,
                        jql: str,
                        fields: List[str],
                        start: int = 0,
                        limit: int = 100) -> Dict[str, Any]:
        """
        Search issues in Jira based on JQL query.

        Args:
            jql (str): Jira Query Language string to filter issues.
            fields (List[str]): List of fields to include in the response.
            start (int, optional): Starting index for pagination. Defaults to 0.
            limit (int, optional): Maximum number of results to fetch. Defaults to 100.

        Returns:
            Dict[str, Any]: JSON response containing issues that match the query.
        """
        payload = {
            'jql': jql,
            'fieldsByKeys': False,
            'fields': fields,
            'startAt': start,
            'maxResults': limit,
        }
        return self.client.request('POST', '/rest/api/3/search', data=payload)

    def __get_issue_changelog(self,
                              issue_id: str,
                              start: int = 0,
                              limit: int = 100) -> Dict[str, Any]:
        """
        Fetch the changelog for a specific issue.

        Args:
            issue_id (str): ID or key of the issue.
            start (int, optional): Starting index for pagination. Defaults to 0.
            limit (int, optional): Maximum number of changelog entries to fetch. Defaults to 100.

        Returns:
            Dict[str, Any]: JSON response containing the issue changelog.
        """
        return self.client.request(method='GET',
                                   path=f'/rest/api/3/issue/{issue_id}/changelog',
                                   params={'startAt': start, 'maxResults': limit})

    def __fetch_issues(self,
                       project_key: str,
                       since: str,
                       start: int = 0,
                       limit: int = 100,
                       custom_fields: Optional[List[str]] = None,
                       updates_only: bool = False) -> Dict[str, Any]:
        """
        Fetch a batch of issues from Jira based on parameters.

        Args:
            project_key (str): Project key to search issues.
            since (str): Date filter in ISO format.
            start (int, optional): Starting index for pagination. Defaults to 0.
            limit (int, optional): Maximum number of results to fetch. Defaults to 100.
            custom_fields (Optional[List[str]], optional): List of custom fields. Defaults to None.
            updates_only (bool, optional): Whether to filter by updates instead of creation. Defaults to False.

        Returns:
            Dict[str, Any]: JSON response containing issues.
        """
        jql = (f'project = {project_key} '
               f'AND {"updated" if updates_only else "created"} >= "{since}" '
               f'ORDER BY created ASC')

        fields = ['parent', 'summary', 'status', 'issuetype', 'created', 'updated']
        if custom_fields:
            fields.extend(custom_fields)

        return self.__search_issues(jql=jql, fields=fields, start=start, limit=limit)

    def __fetch_changelog(self, issue_id: str, start: int = 0, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch the changelog for a specific issue.

        Args:
            issue_id (str): ID or key of the issue.
            start (int, optional): Starting index for pagination. Defaults to 0.
            limit (int, optional): Maximum number of changelog entries to fetch. Defaults to 100.

        Returns:
            Dict[str, Any]: JSON response containing the issue changelog.
        """
        return self.__get_issue_changelog(issue_id, start, limit)

    def __yield_issues(self,
                       project_key: str,
                       since: str,
                       batch: int = 100,
                       custom_fields: Optional[List[str]] = None,
                       updates_only: bool = False) -> Iterator[Dict[str, Any]]:
        """
        Yield issues in batches based on project and date criteria.

        Args:
            project_key (str): Project key to search issues.
            since (str): Date filter in ISO format.
            batch (int, optional): Number of issues to fetch per batch. Defaults to 100.
            custom_fields (Optional[List[str]], optional): List of custom fields. Defaults to None.
            updates_only (bool, optional): Whether to filter by updates instead of creation. Defaults to False.

        Yields:
            Iterator[Dict[str, Any]]: Each issue fetched.
        """
        issues = self.__fetch_issues(project_key=project_key,
                                     since=since,
                                     start=0,
                                     limit=0,
                                     custom_fields=custom_fields,
                                     updates_only=updates_only)

        total = issues.get('total', 0)
        fetched = 0
        while fetched < total:
            j = self.__fetch_issues(project_key=project_key,
                                    since=since,
                                    start=fetched,
                                    limit=batch,
                                    custom_fields=custom_fields,
                                    updates_only=updates_only)

            if not j:
                break
            k = j.get('issues', [])
            if not k:
                break
            for result in k:
                yield result
                fetched += 1

    def __yield_changelog(self, issue_id: str, batch: int = 100) -> Iterator[Dict[str, Any]]:
        """
        Yield changelog entries for a specific issue.

        Args:
            issue_id (str): ID or key of the issue.
            batch (int, optional): Number of changelog entries to fetch per batch. Defaults to 100.

        Yields:
            Iterator[Dict[str, Any]]: Each changelog entry fetched.
        """
        starting_limit = 10
        changelog_items = self.__fetch_changelog(issue_id, start=0, limit=starting_limit)
        total = changelog_items.get('total', 0)
        if total <= starting_limit:
            for result in changelog_items.get('values', []):
                yield result
        else:
            fetched = 0
            while fetched < total:
                j = self.__fetch_changelog(issue_id, start=fetched, limit=batch)
                if not j:
                    break
                k = j.get('values', [])
                if not k:
                    break
                for result in k:
                    yield result
                    fetched += 1

    def fetch_records(self,
                      project_key: str,
                      since: str,
                      custom_fields: Optional[List[str]] = None,
                      updates_only: bool = False) -> Generator[Dict[str, Union[str, None]], None, None]:
        """
        Fetch and yield issues with changelog details.

        Args:
            project_key (str): Project key to search issues.
            since (str): Date filter in ISO format.
            custom_fields (Optional[List[str]], optional): List of custom fields. Defaults to None.
            updates_only (bool, optional): Whether to filter by updates instead of creation. Defaults to False.

        Yields:
            Generator[Dict[str, Union[str, None]], None, None]: Each record with issue and changelog details.
        """

        logger.info(f'Fetching project {project_key} since {since}...')

        # Get high level information fresh every time
        with requests_cache.disabled():
            categories = self.__get_status_categories()
            statuses = self.__get_statuses()
            project = self.__get_project(project_key)
            # Fetch issues' statuses of the project
            project_statuses = self.__get_project_statuses(project_key)

        # Compute lookup tables
        categories_by_category_id = {}
        for category in categories:
            categories_by_category_id[category.get('id')] = category

        status_categories_by_status_id = {}
        for status in statuses:
            status_categories_by_status_id[int(status.get('id'))] = \
                categories_by_category_id[status.get('statusCategory', {}).get('id')]

        issues = self.__yield_issues(project_key=project_key,
                                     since=since,
                                     custom_fields=custom_fields,
                                     updates_only=updates_only)

        for issue in issues:
            logger.info(f"Fetching issue {issue.get('key')}...")

            issue_id = issue.get('id')

            prefix = {
                'project_id': project.get('id'),
                'project_key': project.get('key'),
                'issue_id': issue.get('id'),
                'issue_key': issue.get('key'),
                'issue_type_id': issue.get('fields', {}).get('issuetype', {}).get('id'),
                'issue_type_name': issue.get('fields', {}).get('issuetype', {}).get('name'),
                'issue_title': issue.get('fields', {}).get('summary'),
                'issue_created_date': issue.get('fields', {}).get('created'),
            }

            suffix = {}
            if custom_fields:
                suffix = {k: issue.get('fields', {}).get(k) for k in custom_fields}

            changelog = self.__yield_changelog(issue_id)
            has_status = False
            for change_set in changelog:
                logger.info(f"Fetching changelog for issue {issue.get('key')}...")

                for record in change_set.get('items', []):
                    if record.get('field') == 'status':
                        from_category = status_categories_by_status_id.get(int(record.get('from')), {})
                        to_category = status_categories_by_status_id.get(int(record.get('to')), {})

                        row = {
                            **prefix,
                            'changelog_id': change_set.get('id'),
                            'status_from_id': record.get('from'),
                            'status_from_name': record.get('fromString'),
                            'status_to_id': record.get('to'),
                            'status_to_name': record.get('toString'),
                            'status_from_category_name': from_category.get('name'),
                            'status_to_category_name': to_category.get('name'),
                            'status_change_date': change_set.get('created'),
                            **suffix
                        }

                        yield row
                        has_status = True

            # if we do not have a changelog status for this issue, we should emit a "new" status
            if not has_status:
                yield {
                    **prefix,
                    'changelog_id': None,
                    'status_from_id': None,
                    'status_from_name': None,
                    'status_to_id': None,
                    'status_to_name': None,
                    'status_from_category_name': None,
                    'status_to_category_name': None,
                    'status_change_date': None,
                    **suffix
                }
