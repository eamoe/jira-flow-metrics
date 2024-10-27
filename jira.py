import requests_cache
import sys

from jira.utils.config import Config
from jira.utils.arg_parser import ArgumentParser
from jira.reporting.report_generator import ReportGenerator

from jira.utils.exceptions import (JiraConfigurationError,
                                   JiraConnectionError,
                                   JiraDataExtractionError,
                                   JiraReportGenerationError,
                                   JiraArgumentError)

from jira.utils.logger_config import LoggerConfig


def setup_cache():
    """Set up a requests cache to reduce API calls and improve performance."""
    requests_cache.install_cache(cache_name='jira_cache', backend='sqlite', expire_after=24 * 60 * 60)


def main():
    """Main function to execute the JIRA data extraction and report generation."""

    setup_cache()

    logger_config = LoggerConfig()
    logger = logger_config.get_logger(__name__)

    try:
        config_values = Config().values()
    except JiraConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    try:
        args = ArgumentParser(**config_values).parse()
        logger_config.setup_quiet_logging(args.quiet)
    except JiraArgumentError as e:
        logger.error(f"Argument parsing error: {e}")
        sys.exit(1)

    try:
        report_generator = ReportGenerator(args)
        report_generator.run()
    except (JiraConnectionError,
            JiraDataExtractionError,
            JiraReportGenerationError,
            ValueError) as e:
        logger.error(f"Report generation failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
