import requests_cache
import sys
import logging

from jira.utils.config import Config
from jira.utils.arg_parser import JiraArgumentParser
from jira.reporting.report_generator import JiraReportGenerator

from jira.utils.exceptions import (JiraConfigurationError,
                                   JiraConnectionError,
                                   JiraDataFetchError,
                                   JiraReportGenerationError,
                                   JiraArgumentError)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_cache():
    """Set up a requests cache to reduce API calls and improve performance."""
    requests_cache.install_cache(cache_name='jira_cache', backend='sqlite', expire_after=24 * 60 * 60)


def main():
    setup_cache()

    try:
        config_values = Config().values()
    except JiraConfigurationError as e:
        logger.error(e)
        sys.exit(1)

    try:
        args = JiraArgumentParser(**config_values).parse()
    except JiraArgumentError as e:
        logger.error(e)
        sys.exit(1)

    # Set the global logging level based on --quiet flag
    log_level = logging.WARNING if args.quiet else logging.INFO
    logging.getLogger().setLevel(log_level)  # Set root logger level
    logger.setLevel(log_level)  # Set specific logger level

    try:
        report_generator = JiraReportGenerator(args)
        report_generator.run()
    except (JiraConnectionError,
            JiraDataFetchError,
            JiraReportGenerationError,
            ValueError) as e:
        logger.error(e)
        sys.exit(1)


if __name__ == '__main__':
    main()
