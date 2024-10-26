import requests_cache
import sys
import logging

from jira.utils.config import Config
from jira.utils.arg_parser import JiraArgumentParser
from jira.api.client import ApiClient
from jira.api.fetcher import JiraDataFetcher
from jira.extraction.issue_extractor import JiraIssueExtractor
from jira.reporting.csv_generator import CSVReportGenerator
from jira.utils.exceptions import (JiraConfigurationError,
                                   JiraConnectionError,
                                   JiraDataFetchError,
                                   JiraReportGenerationError,
                                   JiraArgumentError)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_config_values():
    """Load configuration."""
    try:
        values = Config().values()
    except Exception as e:
        raise JiraConfigurationError(f"Error loading configuration: {e}")
    return values


def setup_cache():
    """Set up a requests cache to reduce API calls and improve performance."""
    requests_cache.install_cache(cache_name='jira_cache', backend='sqlite', expire_after=24 * 60 * 60)


def get_custom_fields(args):
    """Return formatted custom fields and names based on arguments."""
    custom_fields = [k if k.startswith('customfield') else 'customfield_{}'.format(k) for k in
                     args.field] if args.field else []
    custom_field_names = list(args.name or []) + custom_fields[len(args.name or []):]
    return custom_fields, custom_field_names


def generate_report(args, custom_fields, custom_field_names):
    """Generate the JIRA CSV report based on the given arguments."""
    logger.info(f'Connecting to {args.domain} with {args.email} email...')

    try:
        client = ApiClient(domain=args.domain, email=args.email, apikey=args.apikey)
    except Exception as e:
        raise JiraConnectionError(f"Failed to connect to JIRA: {e}")

    try:
        fetcher = JiraDataFetcher(client)
        extractor = JiraIssueExtractor(fetcher)
    except Exception as e:
        raise JiraDataFetchError(f"Error fetching data from JIRA: {e}")

    mode = 'a' if args.append else 'w'
    try:
        with open(args.output, mode, newline='') as csv_file:
            csv_generator = CSVReportGenerator(extractor=extractor,
                                               csv_file=csv_file,
                                               project_key=args.project,
                                               since=args.since,
                                               custom_fields=custom_fields,
                                               custom_field_names=custom_field_names,
                                               updates_only=args.updates_only,
                                               anonymize=args.anonymize)
            csv_generator.generate(write_header=not args.append)
    except Exception as e:
        raise JiraReportGenerationError(f"Failed to generate report: {e}")


def main():

    try:
        config_values = get_config_values()
        args = JiraArgumentParser(**config_values).parse()
    except (JiraConfigurationError, JiraArgumentError) as e:
        logger.error(e)
        sys.exit(1)

    # Set the global logging level based on --quiet flag
    log_level = logging.WARNING if args.quiet else logging.INFO
    logging.getLogger().setLevel(log_level)  # Set root logger level
    logger.setLevel(log_level)  # Set specific logger level

    setup_cache()

    try:
        custom_fields, custom_field_names = get_custom_fields(args)
        generate_report(args, custom_fields, custom_field_names)
    except (JiraConfigurationError,
            JiraConnectionError,
            JiraDataFetchError,
            JiraReportGenerationError,
            ValueError) as e:
        logger.error(e)
        sys.exit(1)


if __name__ == '__main__':
    main()
