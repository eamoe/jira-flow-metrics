from ..api.client import ApiClient


class JiraDataFetcher:
    """Responsible for fetching specific data from the Jira API."""
    def __init__(self, client: ApiClient):
        """
        Initialize with an ApiClient instance.

        :param client: ApiClient instance for making requests.
        """
        self.client = client

    def get_status_categories(self):
        """Fetch all status categories from Jira."""
        return self.client.request('GET', '/rest/api/3/statuscategory')

    def get_statuses(self):
        """Fetch all available statuses in Jira."""
        return self.client.request('GET', '/rest/api/3/status')

    def get_project(self, project_key):
        """Fetch details of a specific project by key."""
        return self.client.request('GET', f'/rest/api/3/project/{project_key}')

    def get_project_statuses(self, project_key):
        """Fetch all statuses for a given project."""
        return self.client.request('GET', f'/rest/api/3/project/{project_key}/statuses')

    def search_issues(self, jql, fields, start=0, limit=100):
        """
        Search issues in Jira based on JQL query.

        :param jql: Jira Query Language string to filter issues.
        :param fields: List of fields to include in the response.
        :param start: Starting index for pagination.
        :param limit: Maximum number of results to fetch.
        :return: JSON response containing issues that match the query.
        """
        payload = {
            'jql': jql,
            'fieldsByKeys': False,
            'fields': fields,
            'startAt': start,
            'maxResults': limit,
        }
        return self.client.request('POST', '/rest/api/3/search', data=payload)

    def get_issue_changelog(self, issue_id, start=0, limit=100):
        """
        Fetch the changelog for a specific issue.

        :param issue_id: ID or key of the issue.
        :param start: Starting index for pagination.
        :param limit: Maximum number of changelog entries to fetch.
        :return: JSON response containing the issue changelog.
        """
        return self.client.request(method='GET',
                                   path=f'/rest/api/3/issue/{issue_id}/changelog',
                                   params={'startAt': start, 'maxResults': limit})
