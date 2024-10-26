import logging
import requests_cache

from ..api.fetcher import JiraDataFetcher


class JiraIssueExtractor:
    """Extracts issues and changelogs with logic specific to Jira issues."""
    def __init__(self, fetcher: JiraDataFetcher):
        self.fetcher = fetcher

    def fetch_issues(self, project_key, since, start=0, limit=100, custom_fields=None, updates_only=False):
        jql = (f'project = {project_key} '
               f'AND {"updated" if updates_only else "created"} >= "{since}" '
               f'ORDER BY created ASC')

        fields = ['parent', 'summary', 'status', 'issuetype', 'created', 'updated']
        if custom_fields:
            fields.extend(custom_fields)

        return self.fetcher.search_issues(jql=jql, fields=fields, start=start, limit=limit)

    def fetch_changelog(self, issue_id, start=0, limit=100):
        return self.fetcher.get_issue_changelog(issue_id, start, limit)

    def yield_issues(self, project_key, since, batch=100, custom_fields=None, updates_only=False):
        issues = self.fetch_issues(project_key=project_key,
                                   since=since,
                                   start=0,
                                   limit=0,
                                   custom_fields=custom_fields,
                                   updates_only=updates_only)

        total = issues.get('total', 0)
        fetched = 0
        while fetched < total:
            j = self.fetch_issues(project_key=project_key,
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

    def yield_changelog(self, issue_id, batch=100):
        starting_limit = 10
        changelog_items = self.fetch_changelog(issue_id, start=0, limit=starting_limit)
        total = changelog_items.get('total', 0)
        if total <= starting_limit:
            for result in changelog_items.get('values', []):
                yield result
        else:
            fetched = 0
            while fetched < total:
                j = self.fetch_changelog(issue_id, start=fetched, limit=batch)
                if not j:
                    break
                k = j.get('values', [])
                if not k:
                    break
                for result in k:
                    yield result
                    fetched += 1

    def fetch(self, project_key, since, custom_fields=None, updates_only=False):

        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.info(f'Fetching project {project_key} since {since}...')

        # Get high level information fresh every time
        with requests_cache.disabled():
            categories = self.fetcher.get_status_categories()
            statuses = self.fetcher.get_statuses()
            project = self.fetcher.get_project(project_key)
            # Fetch issues' statuses of the project
            project_statuses = self.fetcher.get_project_statuses(project_key)

        # Compute lookup tables
        categories_by_category_id = {}
        for category in categories:
            categories_by_category_id[category.get('id')] = category

        status_categories_by_status_id = {}
        for status in statuses:
            status_categories_by_status_id[int(status.get('id'))] = \
                categories_by_category_id[status.get('statusCategory', {}).get('id')]

        issues = self.yield_issues(project_key=project_key,
                                   since=since,
                                   custom_fields=custom_fields,
                                   updates_only=updates_only)

        for issue in issues:
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"Fetching issue {issue.get('key')}...")

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

            changelog = self.yield_changelog(issue_id)
            has_status = False
            for change_set in changelog:
                if logging.getLogger().isEnabledFor(logging.INFO):
                    logging.info(f"Fetching changelog for issue {issue.get('key')}...")

                for record in change_set.get('items', []):
                    if record.get('field') == 'status':
                        from_category = status_categories_by_status_id.get(int(record.get('from')), {})
                        to_category = status_categories_by_status_id.get(int(record.get('to')), {})

                        row = dict(prefix)
                        row.update({
                            'changelog_id': change_set.get('id'),
                            'status_from_id': record.get('from'),
                            'status_from_name': record.get('fromString'),
                            'status_to_id': record.get('to'),
                            'status_to_name': record.get('toString'),
                            'status_from_category_name': from_category.get('name'),
                            'status_to_category_name': to_category.get('name'),
                            'status_change_date': change_set.get('created'),
                        })
                        row.update(suffix)

                        yield row

                        has_status = True

            # if we do not have a changelog status for this issue, we should emit a "new" status
            if not has_status:
                row = dict(prefix)
                row.update({
                    'changelog_id': None,
                    'status_from_id': None,
                    'status_from_name': None,
                    'status_to_id': None,
                    'status_to_name': None,
                    'status_from_category_name': None,
                    'status_to_category_name': None,
                    'status_change_date': None
                })
                row.update(suffix)
                yield row
