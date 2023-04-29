from decouple import config
import argparse
from requests.auth import HTTPBasicAuth
import logging
import csv

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


def generate_output_csv(client,
                        csv_file,
                        project_key,
                        since,
                        custom_fields=None,
                        custom_field_names=None,
                        updates_only=False,
                        write_header=False,
                        anonymize=False):

    import datetime
    import dateutil.parser
    import pytz

    field_names = [
        'project_id',
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
        'status_change_date',
    ]

    custom_field_map = {}
    if custom_fields:
        if custom_field_names:
            custom_field_map = dict(zip(custom_fields, custom_field_names))
            field_names.extend(custom_field_names)
        else:
            field_names.extend(custom_fields)

    writer = csv.DictWriter(csv_file, fieldnames=field_names)

    if write_header:
        writer.writeheader()

    count = 0
    records = {}
    for record in records:
        for key, value in record.items():
            # ensure ISO datetime strings with TZ offsets to ISO datetime strings in UTC
            if 'date' in key and value and not isinstance(value, datetime.datetime):
                value = dateutil.parser.parse(value)
                value = value.astimezone(pytz.UTC)
                record[key] = value.isoformat()

        if anonymize:
            record['issue_key'] = record['issue_key'].replace(record['project_key'], 'MSD')
            record['project_key'] = 'MSD'
            record['issue_title'] = 'Masked title'

        if custom_field_map:
            for key, value in custom_field_map.items():
                if key not in record:
                    continue
                record[value] = record[key]
                del record[key]

        writer.writerow(record)
        count += 1


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
