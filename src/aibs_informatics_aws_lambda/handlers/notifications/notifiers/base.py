from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, Optional, Type, Union

from aibs_informatics_core.utils.logging import get_logger

from aibs_informatics_aws_lambda.handlers.notifications.model import NotificationContent
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import (
    NOTIFIER_TARGET,
    NotifierResult,
    NotifierTarget,
)

logger = get_logger(__name__)


@dataclass
class Notifier(Generic[NOTIFIER_TARGET]):
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

    @classmethod
    def notifier_target_class(cls) -> Type[NOTIFIER_TARGET]:
        return cls.__orig_bases__[0].__args__[0]  # type: ignore

    @abstractmethod
    def notify(self, content: NotificationContent, target: NOTIFIER_TARGET) -> NotifierResult:
        raise NotImplementedError("Please implement `notify` method")  # pragma: no cover

    @classmethod
    def parse_target(cls, target: Union[Dict[str, Any], NOTIFIER_TARGET]) -> NOTIFIER_TARGET:
        if isinstance(target, cls.notifier_target_class()):
            return target
        elif isinstance(target, dict):
            return cls.notifier_target_class().from_dict(target)
        else:
            raise ValueError(f"Could not parse target {target} as {cls.notifier_target_class()}")
