import logging
from abc import abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import Generic, Optional, Type

from aibs_informatics_core.utils.logging import get_logger

from aibs_informatics_aws_lambda.handlers.notifications.publishers.model import (
    PublishRequest,
    PublishResponse,
)

logger = get_logger(__name__)


@dataclass
class BasePublisher:
    """
    Base class for all notification handlers

    Example Usage:

    `
    @dataclass
    class SESPublisher(BasePublisher):
        def publish(self, request: PublishRequest) -> PublishResponse:
            return PublishResponse()
    `

    """

    @abstractmethod
    def publish(self, request: PublishRequest) -> PublishResponse:
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    @abstractmethod
    def should_handle(self, request: PublishRequest) -> bool:
        raise NotImplementedError("Please implement should_handle method")
