# Jira Flow Metrics

## Jira Module

Extracts changelog of Jira project issues

Usage:
```commandline
jira.py -h --updates-only --append --anonymize -d DOMAIN -e EMAIL -k APIKEY -o OUTPUT -q -f FIELD_ID -n FIELD_NAME project since
```

### Positional arguments:

| Argument | Description                                              |
|----------|----------------------------------------------------------|
| project  | Jira project from which to extract issues                |
| since    | Date from which to start extracting issues (yyyy-mm-dd)  |

### Options:

| Argument                          | Description                                                                                                                                  |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| -h, --help                        | show this help message and exit                                                                                                              |
| --updates-only                    | When passed, instead of extracting issues created since the since argument, only issues *updated* since the since argument will be extracted |
| --append                          | Append to the output file instead of overwriting it                                                                                          |
| --anonymize                       | Anonymize the data output (no issue titles, project keys, etc)                                                                               |
| -d DOMAIN, --domain DOMAIN        | Jira project domain url (i.e., https://company.atlassian.net). Can also be provided via JIRA_DOMAIN environment variable                     |
| -e EMAIL, --email EMAIL           | Jira user email address for authentication. Can also be provided via JIRA_EMAIL environment variable                                         |
| -k APIKEY, --apikey APIKEY        | Jira user api key for authentication. Can also be provided via JIRA_APIKEY environment variable                                              |
| -o OUTPUT, --output OUTPUT        | File to store the csv output                                                                                                                 |
| -q, --quiet                       | Be quiet and only output warnings to console                                                                                                 |
| -f FIELD_ID, --field FIELD_ID     | Include one or more custom fields in the query by id                                                                                         |
| -n FIELD_NAME, --name FIELD_NAME  | Jira user api key for authentication. Can also be provided via JIRA_APIKEY environment variable                                              |
| -k APIKEY, --apikey APIKEY        | Corresponding output column names for each custom field                                                                                      |
