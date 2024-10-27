import logging


class LoggerConfig:
    def __init__(self):
        """Initialize logging configuration based on quiet mode."""
        self.__setup_default_logging_mode()
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def __setup_default_logging_mode() -> None:
        logging.basicConfig(level=logging.INFO)

    @staticmethod
    def setup_quiet_logging(quiet_mode: bool) -> None:
        """Set up logging configuration."""
        log_level = logging.WARNING if quiet_mode else logging.INFO
        logging.basicConfig(level=log_level)
        logging.getLogger().setLevel(log_level)

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get a logger instance with the specified name."""
        return logging.getLogger(name)
