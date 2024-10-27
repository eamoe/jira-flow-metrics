import requests_cache
import sys
import logging

from jira.utils.config import Config
from jira.utils.arg_parser import JiraArgumentParser
from jira.reporting.report_generator import JiraReportGenerator

from jira.utils.exceptions import (JiraConfigurationError,
                                   JiraConnectionError,
                                   JiraDataExtractionError,
                                   JiraReportGenerationError,
                                   JiraArgumentError)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_cache():
    """Set up a requests cache to reduce API calls and improve performance."""
    requests_cache.install_cache(cache_name='jira_cache', backend='sqlite', expire_after=24 * 60 * 60)


def set_logging_level(quiet_mode: bool) -> None:
    """Set the logging level based on the quiet mode flag."""
    log_level = logging.WARNING if quiet_mode else logging.INFO
    logging.getLogger().setLevel(log_level)
    logger.setLevel(log_level)


def main():
    """Main function to execute the JIRA data extraction and report generation."""

    setup_cache()

    try:
        config_values = Config().values()
    except JiraConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    try:
        args = JiraArgumentParser(**config_values).parse()
    except JiraArgumentError as e:
        logger.error(f"Argument parsing error: {e}")
        sys.exit(1)

    set_logging_level(args.quiet)

    try:
        report_generator = JiraReportGenerator(args)
        report_generator.run()
    except (JiraConnectionError,
            JiraDataExtractionError,
            JiraReportGenerationError,
            ValueError) as e:
        logger.error(f"Report generation failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
