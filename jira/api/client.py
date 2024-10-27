import logging
from requests.auth import HTTPBasicAuth
import requests
from typing import Optional, Dict, Any, List


logger = logging.getLogger(__name__)


class ApiClient:
    """
    Handles API connection and requests.

    Attributes:
        domain (str): The base domain for the API.
        email (str): The user's email for authentication.
        apikey (str): The API key for authentication.
    """
    def __init__(self, domain: str, email: str, apikey: str) -> None:
        """
        Initialize the API client.

        Args:
            domain (str): The base domain for the API.
            email (str): User's email for authentication.
            apikey (str): API key for authentication.
        """
        self.domain = domain.rstrip('/')
        self.email = email
        self.apikey = apikey
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json'}

    def __build_url(self, path: str) -> str:
        """
        Constructs a full URL for a given API path.

        Args:
            path (str): The API endpoint path.

        Returns:
            str: The full URL for the request.
        """
        return f"{self.domain}/{path.lstrip('/')}"

    def __get_auth(self) -> HTTPBasicAuth:
        """
        Returns HTTP basic authentication credentials.

        Returns:
            HTTPBasicAuth: The authentication object for requests.
        """
        return HTTPBasicAuth(self.email, self.apikey)

    def __request(self,
                  method: str,
                  path: str,
                  params: Optional[Dict[str, Any]] = None,
                  data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Makes an API request and returns the JSON response.

        Args:
            method (str): HTTP method ('GET', 'POST').
            path (str): API endpoint path.
            params (Optional[Dict[str, Any]]): Query parameters for the request.
            data (Optional[Dict[str, Any]]): JSON payload for the request.

        Returns:
            Dict[str, Any]: JSON response from the API.

        Raises:
            requests.exceptions.RequestException: If the request fails.
            requests.exceptions.HTTPError: If the HTTP response status is an error.
        """
        url = self.__build_url(path)
        auth = self.__get_auth()

        try:
            response = requests.request(method=method,
                                        url=url,
                                        params=params,
                                        json=data,
                                        headers=self.headers,
                                        auth=auth)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f'HTTP error occurred: {e}. Response: {response.text if response else "No response"}')
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f'Request failed: {e}')
            raise

    def get_status_categories(self) -> Dict[str, Any]:
        """
        Fetch all status categories from Jira.

        Returns:
            Dict[str, Any]: List of status categories.
        """
        return self.__request('GET', '/rest/api/3/statuscategory')

    def get_statuses(self) -> Dict[str, Any]:
        """
        Fetch all available statuses in Jira.

        Returns:
            Dict[str, Any]: List of statuses.
        """
        return self.__request('GET', '/rest/api/3/status')

    def get_project(self, project_key: str) -> Dict[str, Any]:
        """
        Fetch details of a specific project by key.

        Args:
            project_key (str): Key of the project to retrieve.

        Returns:
            Dict[str, Any]: Project details.
        """
        return self.__request('GET', f'/rest/api/3/project/{project_key}')

    def get_project_statuses(self, project_key: str) -> Dict[str, Any]:
        """
        Fetch all statuses for a given project.

        Args:
            project_key (str): Key of the project to retrieve statuses.

        Returns:
            Dict[str, Any]: List of statuses for the project.
        """
        return self.__request('GET', f'/rest/api/3/project/{project_key}/statuses')

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
        return self.__request('POST', '/rest/api/3/search', data=payload)

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
        return self.__request(method='GET',
                              path=f'/rest/api/3/issue/{issue_id}/changelog',
                              params={'startAt': start, 'maxResults': limit})
