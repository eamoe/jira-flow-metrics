# Jira Flow Metrics

## Jira Module

This module allows to interact with Jira APIs to fetch issue data, changelogs, and project details,
and export the data to a CSV file.

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

## Analysis Module

Analyzes exported data

Usage:
```commandline
analysis.py -h -f FILE -o OUTPUT -q --exclude-weekends --exclude-type TYPE --since SINCE --until UNTIL summary / detail / correlation / survival / forecast / shell
```

### Positional arguments:

| Argument     | Description                                                                |
|--------------|----------------------------------------------------------------------------|
| summary      | Generate a summary of metric data (lead time, cycle time, throughput, wip) |
| detail       | Output detailed analysis data                                              |
| correlation  | Test correlation between issue_points and lead/cycle times                 |
| survival     | Analyze the survival of work items                                         |
| forecast     | Forecast the future using Monte Carlo simulation                           |
| shell        | Load the data into an interactive Python shell                             |

### Options:

| Argument                   | Description                                                                     |
|----------------------------|---------------------------------------------------------------------------------|
| -h, --help                 | show this help message and exit                                                 |
| -f FILE, --file FILE       | Data file to analyze (default: stdin)                                           |
| -o OUTPUT, --output OUTPUT | File to output results (default: stdout)                                        |
| -q, --quiet                | Quiet mode that only logs warnings to console                                   |
| --exclude-weekends         | Exclude weekends from cycle and lead time calculations                          |
| --exclude-type TYPE        | Exclude one or more specific types from the analysis (e.g., "Epic", "Bug", etc) |
| --since SINCE              | Only process work items created since date (format: YYYY-MM-DD)                 |
| --until UNTIL              | Only process work items created up until date (format: YYYY-MM-DD)              |
