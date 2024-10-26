from requests.auth import HTTPBasicAuth
import requests
import logging


logger = logging.getLogger(__name__)


class ApiClient:
    """Handles API connection and requests.

    Attributes:
        domain (str): The base domain for the API.
        email (str): The user's email for authentication.
        apikey (str): The API key for authentication.
    """
    def __init__(self, domain, email, apikey):
        self.domain = domain.rstrip('/')
        self.email = email
        self.apikey = apikey
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    def __build_url(self, path):
        """Constructs a full URL for a given API path."""
        return f"{self.domain}/{path.lstrip('/')}"

    def __get_auth(self):
        """Returns HTTP basic authentication credentials."""
        return HTTPBasicAuth(self.email, self.apikey)

    def request(self, method, path, params=None, data=None):
        """Makes an API request and returns the JSON response.

        Args:
            method (str): HTTP method (e.g., 'GET', 'POST', 'PUT').
            path (str): API endpoint path.
            params (dict, optional): Query parameters for the request.
            data (dict, optional): JSON payload for the request.

        Returns:
            dict: JSON response from the API.

        Raises:
            requests.exceptions.RequestException: If the request fails.
            requests.exceptions.HTTPError: If the HTTP response status is an error.
        """
        url = self.__build_url(path)
        auth = self.__get_auth()

        try:
            response = requests.request(method, url, params=params, json=data, headers=self.headers, auth=auth)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f'HTTP error occurred: {e}. Response: {response.text if response else "No response"}')
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f'Request failed: {e}')
            raise
