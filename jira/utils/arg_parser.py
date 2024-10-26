import argparse


class JiraArgumentParser:
    """Handles argument parsing for the Jira data extraction script."""
    def __init__(self, domain, email, apikey, output_file):
        self.domain = domain
        self.email = email
        self.apikey = apikey
        self.output_file = output_file

    def make_parser(self):
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

    def parse_args(self, args=None):
        parser = self.make_parser()
        return parser.parse_args(args)
