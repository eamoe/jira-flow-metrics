from decouple import config
import argparse
from requests.auth import HTTPBasicAuth
import logging

logger = logging.getLogger(__name__)


def headers():
    return {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }


class Client:

    def __init__(self, domain='', email='', apikey=''):
        self.domain = domain
        self.email = email
        self.apikey = apikey

    def url(self, path):
        return self.domain + path

    def auth(self):
        return HTTPBasicAuth(self.email, self.apikey)


def main():

    domain = config('JIRA_DOMAIN')
    email = config('JIRA_EMAIL')
    apikey = config('JIRA_APIKEY')

    parser = argparse.ArgumentParser(description='Extract changelog of Jira project issue')

    parser.add_argument('project', help='Jira project from which to extract issues')
    parser.add_argument('since', help='Date from which to start extracting issues (yyyy-mm-dd)')

    parser.add_argument('--updates-only',
                        action='store_true',
                        help="When passed, instead of extracting issues created since the since argument, only issues "
                             "*updated* since the since argument will be extracted.")
    parser.add_argument('--append',
                        action='store_true',
                        help='Append to the output file instead of overwriting it.')
    parser.add_argument('--anonymize',
                        action='store_true',
                        help='Anonymize the data output (no issue titles, project keys, etc).')

    parser.add_argument('-d', '--domain', default=domain,
                        help='Jira project domain url (i.e., https://company.atlassian.net). Can also be provided via '
                             'JIRA_DOMAIN environment variable.')
    parser.add_argument('-e', '--email', default=email,
                        help='Jira user email address for authentication. Can also be provided via JIRA_EMAIL '
                             'environment variable.')
    parser.add_argument('-k', '--apikey', default=apikey,
                        help='Jira user api key for authentication. Can also be provided via JIRA_APIKEY environment '
                             'variable.')

    parser.add_argument('-o', '--output', default='jira_output_data.csv', help='File to store the csv output.')

    parser.add_argument('-q', '--quiet', action='store_true', help='Be quiet and only output warnings to console.')

    parser.add_argument('-f', '--field', metavar='FIELD_ID', action='append',
                        help='Include one or more custom fields in the query by id.')
    parser.add_argument('-n', '--name', metavar='FIELD_NAME', action='append',
                        help='Corresponding output column names for each custom field.')

    args = parser.parse_args()

    if not args.quiet:
        logging.basicConfig(level=logging.INFO)

    if not all((args.domain, args.email, args.apikey)):
        parser.error("The JIRA_DOMAIN, JIRA_EMAIL, and JIRA_APIKEY environment variables "
                     "must be set or provided via the -d -e -k command line flags.")
        return

    logging.info(f'Connecting to {args.domain} with {args.email} email...')

    client = Client(domain=args.domain, email=args.email, apikey=args.apikey)


if __name__ == '__main__':
    main()
