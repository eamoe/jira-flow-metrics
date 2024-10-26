class JiraConfigurationError(Exception):
    """Exception raised for errors in the JIRA configuration."""
    pass


class JiraConnectionError(Exception):
    """Exception raised when failing to connect to JIRA."""
    pass


class JiraDataFetchError(Exception):
    """Exception raised for errors in the JIRA configuration."""
    pass


class JiraReportGenerationError(Exception):
    """Exception raised when there is an error generating the JIRA report."""
    pass
