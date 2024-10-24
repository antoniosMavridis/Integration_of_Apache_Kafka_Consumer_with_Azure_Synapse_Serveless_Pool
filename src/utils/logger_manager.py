import logging
import os
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

from utils.custom_formatter import CustomFormatter
from utils.custom_filters import ExcludeMessageFilter  


class LoggerManager:
    """
    Manages logging for the application and individual Kafka topics.

    This class provides methods to create and retrieve loggers for application-wide
    logging and topic-specific logging, each with their own log files and formats.
    It ensures that topic-specific logs are also captured in the application logs.
    """

    def __init__(self, base_log_dir: str = 'logging', backup_count: int = 30) -> None:
        """
        Initializes the LoggerManager with the base logging directory.

        Parameters:
            base_log_dir (str): Base directory where all logs are stored.
            backup_count (int): Number of backup log files to keep.
        """
        self.base_log_dir = base_log_dir
        self.backup_count = backup_count
        os.makedirs(self.base_log_dir, exist_ok=True)

        # Initialize the root logger for application-wide logs
        self._initialize_root_logger()

        # Adjust logging levels for specific external libraries
        self._adjust_external_loggers()

        # Apply custom filters to exclude specific messages
        self._apply_custom_filters()

    def _initialize_root_logger(self) -> None:
        """
        Sets up the root logger with application-wide logging handlers.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Check if handlers are already added to prevent duplication
        if not logger.handlers:
            # Define log file path with current date
            current_date = datetime.now().strftime('%Y_%m_%d')
            application_log_dir = os.path.join(self.base_log_dir, 'application_logging')
            os.makedirs(application_log_dir, exist_ok=True)
            log_file = os.path.join(application_log_dir, f'application_{current_date}.log')

            # Create TimedRotatingFileHandler for application logs
            file_handler = TimedRotatingFileHandler(
                log_file,
                when='midnight',
                interval=1,
                backupCount=self.backup_count,
                encoding='utf-8',
                utc=False 
            )
            file_handler.setLevel(logging.INFO)

            # Create StreamHandler for console output
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(logging.INFO)

            # Create and set the custom formatter
            formatter = CustomFormatter(
                '%(asctime)s %(levelname)s %(topic)s %(run_id)s [%(threadName)s] '
                '%(module)s:%(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)
            stream_handler.setFormatter(formatter)

            # Add handlers to the root logger
            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)

    def _adjust_external_loggers(self) -> None:
        """
        Adjusts the logging levels for external libraries to suppress unwanted logs.
        """
        # Suppress INFO logs from Azure SDK
        external_loggers = [
            'azure',
            'azure.storage',
            'azure.storage.filedatalake'
        ]

        for ext_logger_name in external_loggers:
            ext_logger = logging.getLogger(ext_logger_name)
            ext_logger.setLevel(logging.WARNING) 

    def _apply_custom_filters(self) -> None:
        """
        Applies custom filters to the root logger to exclude specific log messages.
        """
        # Define messages or patterns to exclude
        excluded_messages = [
            "Response headers:",  # Exclude logs containing 'Response headers:'
            "Request URL:"       # Exclude logs containing 'Request URL:'
        ]

        # Create an instance of the custom filter
        exclude_filter = ExcludeMessageFilter(excluded_messages)

        # Add the filter to all handlers of the root logger
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            handler.addFilter(exclude_filter)

    def get_topic_logger(self, topic: str, run_id: str) -> logging.LoggerAdapter:
        """
        Sets up and returns a logger adapter for a specific Kafka topic with daily log rotation.

        Parameters:
            topic (str): The Kafka topic name.
            run_id (str): Unique identifier for the run.

        Returns:
            logging.LoggerAdapter: Configured logger adapter for the topic.
        """
        topic_log_dir = os.path.join(self.base_log_dir, f'{topic}_logging')
        os.makedirs(topic_log_dir, exist_ok=True)

        # Define log file path with current date
        current_date = datetime.now().strftime('%Y_%m_%d')
        log_file = os.path.join(topic_log_dir, f'{topic}_{current_date}.log')

        logger = logging.getLogger(f'topic.{topic}')
        logger.setLevel(logging.INFO)
        logger.propagate = True  # Allow logs to propagate to the root logger

        # Prevent adding multiple handlers if the logger already has a handler for the current day
        if not any(
            isinstance(handler, TimedRotatingFileHandler) and
            handler.baseFilename == log_file
            for handler in logger.handlers
        ):
            # Create TimedRotatingFileHandler for topic logs
            file_handler = TimedRotatingFileHandler(
                log_file,
                when='midnight',
                interval=1,
                backupCount=self.backup_count,
                encoding='utf-8',
                utc=False  
            )
            file_handler.setLevel(logging.INFO)

            # Create and set the custom formatter
            formatter = CustomFormatter(
                '%(asctime)s %(levelname)s %(topic)s %(run_id)s [%(threadName)s] '
                '%(module)s:%(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)

            # Add handler to the logger
            logger.addHandler(file_handler)

        # Create a LoggerAdapter to inject 'topic' and 'run_id' into log records
        adapter = logging.LoggerAdapter(logger, {'topic': topic, 'run_id': run_id})

        return adapter
