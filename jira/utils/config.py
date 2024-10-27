from decouple import config

from jira.utils.exceptions import JiraConfigurationError


class Config:
    """Configuration class to handle settings for the application."""

    def __init__(self) -> None:
        self.domain = config('JIRA_DOMAIN', default='https://yourdomain.atlassian.net')
        self.email = config('JIRA_EMAIL', default='your-email@example.com')
        self.apikey = config('JIRA_APIKEY', default='your-api-key')
        self.output_file = config('JIRA_OUTPUT_FILE', default='jira_issues.csv')

    def __get_jira_credentials(self) -> dict:
        """Return the JIRA credentials required for authentication."""
        return {
            'domain': self.domain,
            'email': self.email,
            'apikey': self.apikey
        }

    def __get_output_file(self) -> dict:
        """Return the default output file name for CSV reports."""
        return {
            'output_file': self.output_file
        }

    def values(self) -> dict:
        """Return combined configuration values."""
        try:
            return {
                **self.__get_jira_credentials(),
                **self.__get_output_file()
            }
        except Exception as e:
            raise JiraConfigurationError(f"Error loading configuration: {e}")
