import logging

from jira.utils.exceptions import JiraConnectionError, JiraDataFetchError, JiraReportGenerationError
from jira.api.client import ApiClient
from jira.api.fetcher import JiraDataFetcher
from jira.extraction.issue_extractor import JiraIssueExtractor
from jira.reporting.csv_generator import CSVReportGenerator


logger = logging.getLogger(__name__)


class JiraReportGenerator:
    def __init__(self, args):
        self.args = args
        self.client = None
        self.extractor = None

    def __initialize_client(self):
        """Initialize the JIRA API client."""
        logger.info(f'Connecting to {self.args.domain} with {self.args.email} email...')
        try:
            return ApiClient(domain=self.args.domain, email=self.args.email, apikey=self.args.apikey)
        except Exception as e:
            raise JiraConnectionError(f"Failed to connect to JIRA: {e}")

    def __fetch_data(self):
        """Fetch data from JIRA using the provided client."""
        try:
            fetcher = JiraDataFetcher(self.client)
            return JiraIssueExtractor(fetcher)
        except Exception as e:
            raise JiraDataFetchError(f"Error fetching data from JIRA: {e}")

    def __write_to_csv(self):
        """Write the fetched data to a CSV file."""
        mode = 'a' if self.args.append else 'w'
        try:
            with open(self.args.output, mode, newline='') as csv_file:
                csv_generator = CSVReportGenerator(extractor=self.extractor,
                                                   csv_file=csv_file,
                                                   project_key=self.args.project,
                                                   since=self.args.since,
                                                   custom_fields=self.args.field,
                                                   custom_field_names=self.args.name,
                                                   updates_only=self.args.updates_only,
                                                   anonymize=self.args.anonymize)
                csv_generator.generate(write_header=not self.args.append)
        except Exception as e:
            raise JiraReportGenerationError(f"Failed to generate report: {e}")

    def run(self):
        self.client = self.__initialize_client()
        self.extractor = self.__fetch_data()
        self.__write_to_csv()
