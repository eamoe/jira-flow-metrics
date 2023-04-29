from decouple import config
import argparse
from requests.auth import HTTPBasicAuth
import logging
import csv
import requests_cache
import requests
import json

requests_cache.install_cache('jira_cache', backend='sqlite', expire_after=24 * 60 * 60)

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


def fetch_status_categories_all(client):
    response = requests.get(
        client.url('/rest/api/3/statuscategory'),
        auth=client.auth(),
        headers=headers())
    if response.status_code != 200:
        logging.warning('Could not fetch status categories')
        return {}
    return json.loads(response.text)


def fetch_statuses_all(client):
    response = requests.get(
        client.url('/rest/api/3/status'),
        auth=client.auth(),
        headers=headers())
    if response.status_code != 200:
        logging.warning('Could not fetch statuses')
        return {}
    return json.loads(response.text)


def fetch_project(client, project_key):
    response = requests.get(
        client.url(f'/rest/api/3/project/{project_key}'),
        auth=client.auth(),
        headers=headers())
    if response.status_code != 200:
        logging.warning(f'Could not fetch project {project_key}')
        return {}
    return json.loads(response.text)


def fetch_statuses_by_project(client, project_key):
    response = requests.get(
        client.url(f'/rest/api/3/project/{project_key}/statuses'),
        auth=client.auth(),
        headers=headers())
    if response.status_code != 200:
        logging.warning(f'Could not fetch project {project_key} statuses')
        return {}
    return json.loads(response.text)


def fetch_issues(
        client,
        project_key,
        since,
        start=0,
        limit=1000,
        custom_fields=None,
        updates_only=False,
        use_get=False):

    jql = f'project = {project_key} AND created >= "{since}" ORDER BY created ASC'

    if updates_only:
        jql = f'project = {project_key} AND updated >= "{since}" ORDER BY created ASC'

    fields = [
        'parent',
        'summary',
        'status',
        'issuetype',
        'created',
        'updated'
    ]

    if custom_fields:
        fields = fields + custom_fields

    payload = {
      'jql':            jql,
      'fieldsByKeys':   False,
      'fields':         fields,
      'expand':         'names',
      'startAt':        start,
      'maxResults':     limit,
    }

    if use_get:
        response = requests.request(
           'GET',
           client.url('/rest/api/3/search'),
           params=payload,
           headers=headers(),
           auth=client.auth()
        )
    else:
        response = requests.request(
           'POST',
           client.url('/rest/api/3/search'),
           data=json.dumps(payload),
           headers=headers(),
           auth=client.auth()
        )

    if response.status_code != 200:
        logging.warning(f'Could not fetch issues since {since}')
        return {}

    return json.loads(response.text)


def yield_issues_all(
        client,
        project_key,
        since,
        batch=1000,
        custom_fields=None,
        updates_only=False,
        use_get=False):

    issues_count = fetch_issues(
        client,
        project_key,
        since=since,
        start=0,
        limit=0,
        custom_fields=custom_fields,
        updates_only=updates_only,
        use_get=use_get)

    total = issues_count.get('total', 0)
    fetched = 0
    while fetched < total:
        j = fetch_issues(
            client,
            project_key,
            since=since,
            start=fetched,
            limit=batch,
            custom_fields=custom_fields,
            updates_only=updates_only,
            use_get=use_get)

        if not j:
            break
        k = j.get('issues', [])
        if not k:
            break
        for result in k:
            yield result
            fetched += 1


def fetch_changelog(
        client,
        issue_id,
        start,
        limit):

    params = {
        'startAt':      start,
        'maxResults':   limit
    }

    response = requests.request(
        'GET',
        client.url(f'/rest/api/3/issue/{issue_id}/changelog'),
        params=params,
        auth=client.auth(),
        headers=headers())

    if response.status_code != 200:
        logging.warning(f'Could not fetch changelog for issue {issue_id}')
        return {}
    return json.loads(response.text)


def yield_changelog_all(
        client,
        issue_id,
        batch=1000):

    starting_limit = 10
    changelog_count = fetch_changelog(
        client,
        issue_id,
        start=0,
        limit=starting_limit)
    total = changelog_count.get('total', 0)
    if total <= starting_limit:
        for result in changelog_count.get('values', []):
            yield result
    else:
        fetched = 0
        while fetched < total:
            j = fetch_changelog(
                client,
                issue_id,
                start=fetched,
                limit=batch)
            if not j:
                break
            k = j.get('values', [])
            if not k:
                break
            for result in k:
                yield result
                fetched += 1


def fetch(client,
          project_key,
          since,
          custom_fields=None,
          updates_only=False):
    logging.info(f'Fetching project {project_key} since {since}...')

    # Get high level information fresh every time
    with requests_cache.disabled():
        categories = fetch_status_categories_all(client)
        statuses = fetch_statuses_all(client)
        project = fetch_project(client, project_key)
        project_statuses = fetch_statuses_by_project(client, project_key)

    # Compute lookup tables
    categories_by_category_id = {}
    for category in categories:
        categories_by_category_id[category.get('id')] = category

    status_categories_by_status_id = {}
    for status in statuses:
        status_categories_by_status_id[int(status.get('id'))] = \
            categories_by_category_id[status.get('statusCategory', {}).get('id')]

    issues = yield_issues_all(
        client,
        project_key,
        since=since,
        custom_fields=custom_fields,
        updates_only=updates_only,
        use_get=True)

    for issue in issues:
        logging.info(f"Fetching issue {issue.get('key')}...")

        issue_id = issue.get('id')

        prefix = {
            'project_id':           project.get('id'),
            'project_key':          project.get('key'),
            'issue_id':             issue.get('id'),
            'issue_key':            issue.get('key'),
            'issue_type_id':        issue.get('fields', {}).get('issuetype', {}).get('id'),
            'issue_type_name':      issue.get('fields', {}).get('issuetype', {}).get('name'),
            'issue_title':          issue.get('fields', {}).get('summary'),
            'issue_created_date':   issue.get('fields', {}).get('created'),
        }

        suffix = {}
        if custom_fields:
            suffix = {k: issue.get('fields', {}).get(k) for k in custom_fields}

        changelog = yield_changelog_all(client, issue_id)
        has_status = False
        for change_set in changelog:
            logging.info(f"Fetching changelog for issue {issue.get('key')}...")

            for record in change_set.get('items', []):
                if record.get('field') == 'status':
                    from_category = status_categories_by_status_id.get(int(record.get('from')), {})
                    to_category = status_categories_by_status_id.get(int(record.get('to')), {})

                    row = dict(prefix)
                    row.update({
                        'changelog_id':                 change_set.get('id'),
                        'status_from_id':               record.get('from'),
                        'status_from_name':             record.get('fromString'),
                        'status_to_id':                 record.get('to'),
                        'status_to_name':               record.get('toString'),
                        'status_from_category_name':    from_category.get('name'),
                        'status_to_category_name':      to_category.get('name'),
                        'status_change_date':           change_set.get('created'),
                    })
                    row.update(suffix)

                    yield row

                    has_status = True

        # if we do not have a changelog status for this issue, we should emit a "new" status
        if not has_status:
            row = dict(prefix)
            row.update({
                'changelog_id':                 None,
                'status_from_id':               None,
                'status_from_name':             None,
                'status_to_id':                 None,
                'status_to_name':               None,
                'status_from_category_name':    None,
                'status_to_category_name':      None,
                'status_change_date':           None
            })
            row.update(suffix)
            yield row


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

    records = fetch(client,
                    project_key,
                    since=since,
                    custom_fields=custom_fields,
                    updates_only=updates_only)
    count = 0
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

    logging.info(f'{count} records written')


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

    mode = 'a' if args.append else 'w'

    custom_fields = [k if k.startswith('customfield') else 'customfield_{}'.format(k) for k in
                     args.field] if args.field else []
    custom_field_names = list(args.name or []) + custom_fields[len(args.name or []):]

    with open(args.output, mode, newline='') as csv_file:
        logging.info(f'{args.output} Opened for writing (mode: {mode})...')
        generate_output_csv(
            client,
            csv_file,
            args.project,
            since=args.since,
            custom_fields=custom_fields,
            custom_field_names=custom_field_names,
            updates_only=args.updates_only,
            write_header=not args.append,
            anonymize=args.anonymize)


if __name__ == '__main__':
    main()
