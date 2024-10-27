import logging
import csv
import datetime
import dateutil.parser
import pytz

from jira.api.client import ApiClient
from jira.api.fetcher import JiraDataFetcher
from jira.extraction.issue_extractor import JiraIssueExtractor

from jira.utils.exceptions import JiraConnectionError, JiraDataFetchError, JiraReportGenerationError


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

    def __build_field_names(self):
        default_fields = ['project_id',
                          'project_key',
                          'issue_id',
                          'issue_key',
                          'issue_type_id',
                          'issue_type_name',
                          'issue_title',
                          'issue_created_date',
                          'changelog_id',
                          'status_from_id',
                          'status_from_name',
                          'status_to_id',
                          'status_to_name',
                          'status_from_category_name',
                          'status_to_category_name',
                          'status_change_date']
        # Append custom field names or ids
        return default_fields + (self.args.name or self.args.field)

    @staticmethod
    def __parse_dates(record):
        """Convert date fields to ISO format."""
        for key, value in record.items():
            if 'date' in key and value and not isinstance(value, datetime.datetime):
                record[key] = dateutil.parser.parse(value).astimezone(pytz.UTC).isoformat()
        return record

    @staticmethod
    def __anonymize_record(record):
        """Anonymize sensitive fields in the record."""
        record['issue_key'] = record['issue_key'].replace(record['project_key'], 'ANON')
        record['project_key'] = 'ANON'
        record['issue_title'] = 'Anonymized Title'
        return record

    def __map_custom_fields(self, record):
        """Map custom fields to their corresponding names if provided."""
        custom_field_map = dict(zip(self.args.field, self.args.name))
        for field_id, field_name in custom_field_map.items():
            if field_id in record:
                record[field_name] = record.pop(field_id)
        return record

    def __write_to_csv(self, extractor):
        """Write the fetched data to a CSV file."""
        mode = 'a' if self.args.append else 'w'
        field_names = self.__build_field_names()
        try:
            with open(self.args.output, mode, newline='') as csv_file:
                writer = csv.DictWriter(f=csv_file, fieldnames=field_names)
                if not self.args.append:
                    writer.writeheader()

                records = extractor.fetch(project_key=self.args.project,
                                          since=self.args.since,
                                          custom_fields=self.args.field,
                                          updates_only=self.args.updates_only)

                count = 0
                for record in records:
                    record = self.__parse_dates(record)

                    if self.args.anonymize:
                        record = self.__anonymize_record(record)

                    if self.args.field:
                        record = self.__map_custom_fields(record)

                    writer.writerow(record)
                    count += 1

                logging.info(f'{count} records written')

        except Exception as e:
            raise JiraReportGenerationError(f"Failed to generate report: {e}")

    def run(self):
        self.client = self.__initialize_client()
        self.extractor = self.__fetch_data()
        self.__write_to_csv(self.extractor)
