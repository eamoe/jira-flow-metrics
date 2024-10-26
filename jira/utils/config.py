from decouple import config


class Config:
    """Configuration class to handle settings for the application."""

    def __init__(self):
        self.domain = config('JIRA_DOMAIN', default='https://yourdomain.atlassian.net')
        self.email = config('JIRA_EMAIL', default='your-email@example.com')
        self.apikey = config('JIRA_APIKEY', default='your-api-key')
        self.output_file = config('JIRA_OUTPUT_FILE', default='jira_issues.csv')

    def get_jira_credentials(self):
        """Return the JIRA credentials required for authentication."""
        return {
            'domain': self.domain,
            'email': self.email,
            'apikey': self.apikey
        }

    def get_output_file(self):
        """Return the default output file name for CSV reports."""
        return {
            'output_file': self.output_file
        }
