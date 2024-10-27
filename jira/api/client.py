from requests.auth import HTTPBasicAuth
import requests
import logging
from typing import Optional, Dict, Any


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

    def request(self,
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
