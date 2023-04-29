from decouple import config


def main():
    domain = config('JIRA_DOMAIN')
    email = config('JIRA_EMAIL')
    apikey = config('JIRA_APIKEY')


if __name__ == '__main__':
    main()
