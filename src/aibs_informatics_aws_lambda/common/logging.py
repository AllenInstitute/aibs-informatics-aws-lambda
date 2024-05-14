import logging
from typing import Optional, Union

from aibs_informatics_core.utils.logging import get_all_handlers
from aws_lambda_powertools.logging import Logger

from aibs_informatics_aws_lambda.common.base import HandlerMixins

SERVICE_NAME = "aibs"


logger = Logger(service=SERVICE_NAME, child=True)


LOGGING_ATTR = "_logging"


class LoggingMixins(HandlerMixins):
    @property
    def log(self) -> Logger:
        return self.logger

    @log.setter
    def log(self, value: Logger):
        self.logger = value

    @property
    def logger(self) -> Logger:
        try:
            return self._logger
        except AttributeError:
            self.logger = self.get_logger(self.service_name())
        return self.logger

    @logger.setter
    def logger(self, value: Logger):
        self._logger = value

    @classmethod
    def get_logger(cls, service: Optional[str] = None, add_to_root: bool = False) -> Logger:
        return get_service_logger(service=service, add_to_root=add_to_root)

    def add_logger_to_root(self):
        add_handler_to_logger(self.logger, None)


def get_service_logger(
    service: Optional[str] = None, child: bool = False, add_to_root: bool = False
) -> Logger:
    service_logger = Logger(service=service, child=child)
    if add_to_root:
        add_handler_to_logger(service_logger)
    return service_logger


def add_handler_to_logger(
    source_logger: Logger, target_logger: Union[str, logging.Logger, None] = None
):
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
