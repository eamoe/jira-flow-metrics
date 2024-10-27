from typing import Any, Dict, List, Optional

from ..api.client import ApiClient


class JiraDataFetcher:
    """
    Responsible for fetching specific data from the Jira API.
    """

    def __init__(self, client: ApiClient) -> None:
        """
        Initialize with an ApiClient instance.

        Args:
            client (ApiClient): Instance for making requests.
        """
        self.client = client

    def get_status_categories(self) -> Dict[str, Any]:
        """
        Fetch all status categories from Jira.

        Returns:
            Dict[str, Any]: List of status categories.
        """
        return self.client.request('GET', '/rest/api/3/statuscategory')

    def get_statuses(self) -> Dict[str, Any]:
        """
        Fetch all available statuses in Jira.

        Returns:
            Dict[str, Any]: List of statuses.
        """
        return self.client.request('GET', '/rest/api/3/status')

    def get_project(self, project_key: str) -> Dict[str, Any]:
        """
        Fetch details of a specific project by key.

        Args:
            project_key (str): Key of the project to retrieve.

        Returns:
            Dict[str, Any]: Project details.
        """
        return self.client.request('GET', f'/rest/api/3/project/{project_key}')

    def get_project_statuses(self, project_key: str) -> Dict[str, Any]:
        """
        Fetch all statuses for a given project.

        Args:
            project_key (str): Key of the project to retrieve statuses.

        Returns:
            Dict[str, Any]: List of statuses for the project.
        """
        return self.client.request('GET', f'/rest/api/3/project/{project_key}/statuses')

    def search_issues(self,
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

    def get_issue_changelog(self,
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
