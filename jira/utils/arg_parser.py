import argparse
import datetime
from typing import List, Tuple

from .exceptions import JiraArgumentError


class ParsedArgs:
    def __init__(self, namespace: argparse.Namespace) -> None:
        self.project: str = namespace.project
        self.since: str = namespace.since
        self.updates_only: bool = namespace.updates_only
        self.append: bool = namespace.append
        self.anonymize: bool = namespace.anonymize
        self.domain: str = namespace.domain
        self.email: str = namespace.email
        self.apikey: str = namespace.apikey
        self.output: str = namespace.output
        self.quiet: bool = namespace.quiet
        self.field: List[str] = namespace.field
        self.name: List[str] = namespace.name

    def validate(self) -> None:
        """Validate the parsed arguments."""
        if not all((self.domain, self.email, self.apikey)):
            raise JiraArgumentError("The JIRA_DOMAIN, JIRA_EMAIL, and JIRA_APIKEY must be provided.")

        if not self.__validate_date_format(self.since):
            raise JiraArgumentError(f"Invalid date format for '{self.since}'. It should be in the format YYYY-MM-DD.")

    @staticmethod
    def __validate_date_format(date_string: str) -> bool:
        try:
            datetime.datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False


class JiraArgumentParser:
    """Handles argument parsing for the Jira data extraction script."""
    def __init__(self, domain: str, email: str, apikey: str, output_file: str) -> None:
        self.domain = domain
        self.email = email
        self.apikey = apikey
        self.output_file = output_file

    def __make_parser(self) -> argparse.ArgumentParser:
        """Create and configure the argument parser."""
        parser = argparse.ArgumentParser(description='Extract changelog of Jira project issue')
        parser.add_argument('project',
                            help='Jira project from which to extract issues')
        parser.add_argument('since',
                            help='Date from which to start extracting issues (yyyy-mm-dd)')
        parser.add_argument('--updates-only',
                            action='store_true',
                            help="When passed, instead of extracting issues created since the since argument, "
                                 "only issues *updated* since the since argument will be extracted.")
        parser.add_argument('--append',
                            action='store_true',
                            help='Append to the output file instead of overwriting it.')
        parser.add_argument('--anonymize',
                            action='store_true',
                            help='Anonymize the data output (no issue titles, project keys, etc).')
        parser.add_argument('-d', '--domain',
                            default=self.domain,
                            help='Jira project domain url (i.e., https://company.atlassian.net). '
                                 'Can also be provided via JIRA_DOMAIN environment variable.')
        parser.add_argument('-e', '--email',
                            default=self.email,
                            help='Jira user email address for authentication. '
                                 'Can also be provided via JIRA_EMAIL environment variable.')
        parser.add_argument('-k', '--apikey',
                            default=self.apikey,
                            help='Jira user api key for authentication. '
                                 'Can also be provided via JIRA_APIKEY environment variable.')
        parser.add_argument('-o', '--output',
                            default=self.output_file,
                            help='File to store the csv output.')
        parser.add_argument('-q', '--quiet',
                            action='store_true',
                            help='Be quiet and only output warnings to console.')
        parser.add_argument('-f', '--field',
                            metavar='FIELD_ID',
                            action='append',
                            help='Include one or more custom fields in the query by id.')
        parser.add_argument('-n', '--name',
                            metavar='FIELD_NAME',
                            action='append',
                            help='Corresponding output column names for each custom field.')

        return parser

    @staticmethod
    def __get_custom_fields(namespace: argparse.Namespace) -> Tuple[List[str], List[str]]:
        """Return formatted custom fields and names based on arguments."""
        field_ids = [k if k.startswith('customfield') else 'customfield_{}'.format(k) for k in
                     namespace.field] if namespace.field else []
        field_names = list(namespace.name or []) + field_ids[len(namespace.name or []):]
        return field_ids, field_names

    def parse(self) -> ParsedArgs:
        """Parse the command line arguments using the created parser."""
        parser = self.__make_parser()
        try:
            namespace = parser.parse_args()
            namespace.field, namespace.name = self.__get_custom_fields(namespace)
            parsed_args = ParsedArgs(namespace)
            parsed_args.validate()
            return parsed_args
        except (argparse.ArgumentError, JiraArgumentError) as e:
            raise JiraArgumentError(f"Argument parsing error: {e}")
