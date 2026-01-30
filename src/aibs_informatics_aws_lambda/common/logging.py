"""Logging utilities for AWS Lambda handlers.

Provides logging mixins and helper functions for configuring
structured logging with AWS Lambda Powertools.
"""

import logging
from typing import Optional, Union

from aibs_informatics_core.utils.logging import get_all_handlers
from aws_lambda_powertools.logging import Logger

from aibs_informatics_aws_lambda.common.base import HandlerMixins

SERVICE_NAME = "aibs"


logger = Logger(service=SERVICE_NAME, child=True)


LOGGING_ATTR = "_logging"


class LoggingMixins(HandlerMixins):
    """Mixin class providing structured logging capabilities.

    Integrates AWS Lambda Powertools Logger for structured JSON logging
    with automatic context injection and correlation IDs.

    Attributes:
        log: Alias for the logger property.
        logger: The AWS Lambda Powertools Logger instance.
    """
    @property
    def log(self) -> Logger:
        """Alias for the logger property.

        Returns:
            The configured Logger instance.
        """
        return self.logger

    @log.setter
    def log(self, value: Logger):
        """Set the logger instance.

        Args:
            value (Logger): The Logger instance to set.
        """
        self.logger = value

    @property
    def logger(self) -> Logger:
        """Get the Logger instance, creating one if needed.

        Returns:
            The configured Logger instance for this handler.
        """
        try:
            return self._logger
        except AttributeError:
            self.logger = self.get_logger(self.service_name())
        return self.logger

    @logger.setter
    def logger(self, value: Logger):
        """Set the logger instance.

        Args:
            value (Logger): The Logger instance to set.
        """
        self._logger = value

    @classmethod
    def get_logger(cls, service: Optional[str] = None, add_to_root: bool = False) -> Logger:
        """Create a new Logger instance.

        Args:
            service (Optional[str]): The service name for the logger. If None, uses default.
            add_to_root (bool): Whether to add the logger handler to the root logger.

        Returns:
            A configured Logger instance.
        """
        return get_service_logger(service=service, add_to_root=add_to_root)

    def add_logger_to_root(self):
        """Add this handler's logger to the root logger.

        Ensures log messages from other modules are captured with
        the same structured format.
        """
        add_handler_to_logger(self.logger, None)


def get_service_logger(
    service: Optional[str] = None, child: bool = False, add_to_root: bool = False
) -> Logger:
    """Create a service logger with optional root logger integration.

    Args:
        service (Optional[str]): The service name for the logger. If None, uses default.
        child (bool): Whether to create a child logger.
        add_to_root (bool): Whether to add the logger handler to the root logger.

    Returns:
        A configured Logger instance for the service.
    """
    service_logger = Logger(service=service, child=child)
    if add_to_root:
        add_handler_to_logger(service_logger)
    return service_logger


def add_handler_to_logger(
    source_logger: Logger, target_logger: Union[str, logging.Logger, None] = None
):
    """Add a source logger's handler to a target logger.

    Copies the handler from the source logger to the target logger,
    ensuring consistent log formatting across loggers.

    Args:
        source_logger (Logger): The Logger whose handler will be copied.
        target_logger (Union[str, logging.Logger, None]): The target logger to receive the handler.
            Can be a logger name string, a Logger instance, or None
            for the root logger.
    """
    handler = source_logger.registered_handler

    if target_logger is None or isinstance(target_logger, str):
        target_logger = logging.getLogger(target_logger)
        log_level = min(source_logger.log_level, target_logger.getEffectiveLevel())
        target_logger.setLevel(log_level)
    target_logger_handlers = get_all_handlers(target_logger)

    # TODO: This is not avoiding duplicate handlers.
    # we need to have better comparison logic
    if handler not in target_logger_handlers:
        target_logger.addHandler(handler)
