from decouple import config
import argparse
from requests.auth import HTTPBasicAuth
import logging
import csv
import requests_cache
import requests
import json
import datetime
import dateutil.parser
import pytz


requests_cache.install_cache(cache_name='jira_cache', backend='sqlite', expire_after=24 * 60 * 60)

logger = logging.getLogger(__name__)


class ApiClient:
    """Handles the API connection and requests."""
    def __init__(self, domain, email, apikey):
        self.domain = domain
        self.email = email
        self.apikey = apikey

    def _build_url(self, path):
        return self.domain + path

    def _get_auth(self):
        return HTTPBasicAuth(self.email, self.apikey)

    def request(self, method, path, params=None, data=None):
        url = self._build_url(path)
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        auth = self._get_auth()

        response = requests.request(method, url, params=params, json=data, headers=headers, auth=auth)
        if response.status_code != 200:
            logging.warning(f'Error fetching data from {url}: {response.status_code}')
            return {}

        return json.loads(response.text)


class JiraDataFetcher:
    """Responsible for fetching specific data from the Jira API."""
    def __init__(self, client: ApiClient):
        self.client = client

    def get_status_categories(self):
        return self.client.request('GET', '/rest/api/3/statuscategory')

    def get_statuses(self):
        return self.client.request('GET', '/rest/api/3/status')

    def get_project(self, project_key):
        return self.client.request('GET', f'/rest/api/3/project/{project_key}')

    def get_project_statuses(self, project_key):
        return self.client.request('GET', f'/rest/api/3/project/{project_key}/statuses')

    def search_issues(self, jql, fields, start=0, limit=100):
        payload = {
            'jql': jql,
            'fieldsByKeys': False,
            'fields': fields,
            'startAt': start,
            'maxResults': limit,
        }
        return self.client.request('POST', '/rest/api/3/search', data=payload)

    def get_issue_changelog(self, issue_id, start=0, limit=100):
        return self.client.request(method='GET',
                                   path=f'/rest/api/3/issue/{issue_id}/changelog',
                                   params={'startAt': start, 'maxResults': limit})


class JiraIssueExtractor:
    """Extracts issues and changelogs with logic specific to Jira issues."""
    def __init__(self, fetcher: JiraDataFetcher):
        self.fetcher = fetcher

    def fetch_issues(self, project_key, since, start=0, limit=100, custom_fields=None, updates_only=False):
        jql = (f'project = {project_key} '
               f'AND {"updated" if updates_only else "created"} >= "{since}" '
               f'ORDER BY created ASC')

        fields = ['parent', 'summary', 'status', 'issuetype', 'created', 'updated']
        if custom_fields:
            fields.extend(custom_fields)

        return self.fetcher.search_issues(jql=jql, fields=fields, start=start, limit=limit)

    def fetch_changelog(self, issue_id, start=0, limit=100):
        return self.fetcher.get_issue_changelog(issue_id, start, limit)

    def yield_issues(self, project_key, since, batch=100, custom_fields=None, updates_only=False):
        issues = self.fetch_issues(project_key=project_key,
                                   since=since,
                                   start=0,
                                   limit=0,
                                   custom_fields=custom_fields,
                                   updates_only=updates_only)

        total = issues.get('total', 0)
        fetched = 0
        while fetched < total:
            j = self.fetch_issues(project_key=project_key,
                                  since=since,
                                  start=fetched,
                                  limit=batch,
                                  custom_fields=custom_fields,
                                  updates_only=updates_only)

            if not j:
                break
            k = j.get('issues', [])
            if not k:
                break
            for result in k:
                yield result
                fetched += 1

    def yield_changelog(self, issue_id, batch=100):
        starting_limit = 10
        changelog_items = self.fetch_changelog(issue_id, start=0, limit=starting_limit)
        total = changelog_items.get('total', 0)
        if total <= starting_limit:
            for result in changelog_items.get('values', []):
                yield result
        else:
            fetched = 0
            while fetched < total:
                j = self.fetch_changelog(issue_id, start=fetched, limit=batch)
                if not j:
                    break
                k = j.get('values', [])
                if not k:
                    break
                for result in k:
                    yield result
                    fetched += 1

    def fetch(self, project_key, since, custom_fields=None, updates_only=False):

        logging.info(f'Fetching project {project_key} since {since}...')

        # Get high level information fresh every time
        with requests_cache.disabled():
            categories = self.fetcher.get_status_categories()
            statuses = self.fetcher.get_statuses()
            project = self.fetcher.get_project(project_key)
            # Fetch issues' statuses of the project
            project_statuses = self.fetcher.get_project_statuses(project_key)

        # Compute lookup tables
        categories_by_category_id = {}
        for category in categories:
            categories_by_category_id[category.get('id')] = category

        status_categories_by_status_id = {}
        for status in statuses:
            status_categories_by_status_id[int(status.get('id'))] = \
                categories_by_category_id[status.get('statusCategory', {}).get('id')]

        issues = self.yield_issues(project_key=project_key,
                                   since=since,
                                   custom_fields=custom_fields,
                                   updates_only=updates_only)

        for issue in issues:
            logging.info(f"Fetching issue {issue.get('key')}...")

            issue_id = issue.get('id')

            prefix = {
                'project_id': project.get('id'),
                'project_key': project.get('key'),
                'issue_id': issue.get('id'),
                'issue_key': issue.get('key'),
                'issue_type_id': issue.get('fields', {}).get('issuetype', {}).get('id'),
                'issue_type_name': issue.get('fields', {}).get('issuetype', {}).get('name'),
                'issue_title': issue.get('fields', {}).get('summary'),
                'issue_created_date': issue.get('fields', {}).get('created'),
            }

            suffix = {}
            if custom_fields:
                suffix = {k: issue.get('fields', {}).get(k) for k in custom_fields}

            changelog = self.yield_changelog(issue_id)
            has_status = False
            for change_set in changelog:
                logging.info(f"Fetching changelog for issue {issue.get('key')}...")

                for record in change_set.get('items', []):
                    if record.get('field') == 'status':
                        from_category = status_categories_by_status_id.get(int(record.get('from')), {})
                        to_category = status_categories_by_status_id.get(int(record.get('to')), {})

                        row = dict(prefix)
                        row.update({
                            'changelog_id': change_set.get('id'),
                            'status_from_id': record.get('from'),
                            'status_from_name': record.get('fromString'),
                            'status_to_id': record.get('to'),
                            'status_to_name': record.get('toString'),
                            'status_from_category_name': from_category.get('name'),
                            'status_to_category_name': to_category.get('name'),
                            'status_change_date': change_set.get('created'),
                        })
                        row.update(suffix)

                        yield row

                        has_status = True

            # if we do not have a changelog status for this issue, we should emit a "new" status
            if not has_status:
                row = dict(prefix)
                row.update({
                    'changelog_id': None,
                    'status_from_id': None,
                    'status_from_name': None,
                    'status_to_id': None,
                    'status_to_name': None,
                    'status_from_category_name': None,
                    'status_to_category_name': None,
                    'status_change_date': None
                })
                row.update(suffix)
                yield row


class CSVReportGenerator:
    def __init__(self,
                 extractor,
                 csv_file,
                 project_key,
                 since,
                 custom_fields=None,
                 custom_field_names=None,
                 updates_only=False,
                 anonymize=False):
        self.extractor = extractor
        self.csv_file = csv_file
        self.project_key = project_key
        self.since = since
        self.custom_fields = custom_fields
        self.custom_field_names = custom_field_names
        self.updates_only = updates_only
        self.anonymize = anonymize
        self.field_names = ['project_id',
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
        if self.custom_fields:
            self.custom_field_map = dict(zip(custom_fields, custom_field_names)) if custom_field_names else {}
            self.field_names.extend(self.custom_field_names or self.custom_fields)
        else:
            self.custom_field_map = {}

    def generate(self, write_header=False):
        writer = csv.DictWriter(f=self.csv_file, fieldnames=self.field_names)
        if write_header:
            writer.writeheader()

        records = self.extractor.fetch(project_key=self.project_key,
                                       since=self.since,
                                       custom_fields=self.custom_fields,
                                       updates_only=self.updates_only)

        count = 0
        for record in records:
            for key, value in record.items():
                if 'date' in key and value and not isinstance(value, datetime.datetime):
                    value = dateutil.parser.parse(value).astimezone(pytz.UTC)
                    record[key] = value.isoformat()

            if self.anonymize:
                record['issue_key'] = record['issue_key'].replace(record['project_key'], 'ANON')
                record['project_key'] = 'ANON'
                record['issue_title'] = 'Anonymized Title'

            if self.custom_field_map:
                for key, value in self.custom_field_map.items():
                    if key in record:
                        record[value] = record.pop(key)

            writer.writerow(record)
            count += 1

        logging.info(f'{count} records written')


def make_parser(domain, email, apikey, output_file):
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
                        default=domain,
                        help='Jira project domain url (i.e., https://company.atlassian.net). '
                             'Can also be provided via JIRA_DOMAIN environment variable.')
    parser.add_argument('-e', '--email',
                        default=email,
                        help='Jira user email address for authentication. '
                             'Can also be provided via JIRA_EMAIL environment variable.')
    parser.add_argument('-k', '--apikey',
                        default=apikey,
                        help='Jira user api key for authentication. '
                             'Can also be provided via JIRA_APIKEY environment variable.')
    parser.add_argument('-o', '--output',
                        default=output_file,
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


def main():

    domain = config('JIRA_DOMAIN')
    email = config('JIRA_EMAIL')
    apikey = config('JIRA_APIKEY')
    output_file = config('JIRA_OUTPUT_FILE')

    parser = make_parser(domain=domain, email=email, apikey=apikey, output_file=output_file)
    args = parser.parse_args()

    if not args.quiet:
        logging.basicConfig(level=logging.INFO)

    if not all((args.domain, args.email, args.apikey)):
        parser.error("The JIRA_DOMAIN, JIRA_EMAIL, and JIRA_APIKEY environment variables "
                     "must be set or provided via the -d -e -k command line flags.")
        return

    logging.info(f'Connecting to {args.domain} with {args.email} email...')

    client = ApiClient(domain=args.domain, email=args.email, apikey=args.apikey)
    fetcher = JiraDataFetcher(client)
    extractor = JiraIssueExtractor(fetcher)

    mode = 'a' if args.append else 'w'

    custom_fields = [k if k.startswith('customfield') else 'customfield_{}'.format(k) for k in
                     args.field] if args.field else []
    custom_field_names = list(args.name or []) + custom_fields[len(args.name or []):]

    with open(args.output, mode, newline='') as csv_file:
        logging.info(f'{args.output} Opened for writing (mode: {mode})...')
        csv_generator = CSVReportGenerator(extractor=extractor,
                                           csv_file=csv_file,
                                           project_key=args.project,
                                           since=args.since,
                                           custom_fields=custom_fields,
                                           custom_field_names=custom_field_names,
                                           updates_only=args.updates_only,
                                           anonymize=args.anonymize)
        csv_generator.generate(write_header=not args.append)


if __name__ == '__main__':
    main()
