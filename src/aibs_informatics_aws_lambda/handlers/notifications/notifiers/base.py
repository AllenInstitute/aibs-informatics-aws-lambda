"""Base notifier class for notification delivery.

Provides the abstract base class for implementing notification
delivery to different channels.
"""

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, Type, Union

from aibs_informatics_core.utils.logging import get_logger

from aibs_informatics_aws_lambda.handlers.notifications.model import NotificationContent
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import (
    NOTIFIER_TARGET,
    NotifierResult,
)

logger = get_logger(__name__)


@dataclass
class Notifier(Generic[NOTIFIER_TARGET]):
    """Abstract base class for notification delivery implementations.

    Defines the interface for sending notifications to specific targets.
    Subclasses implement the actual delivery logic for different channels
    (e.g., SES, SNS).

    Type Parameters:
        NOTIFIER_TARGET: The target model type for this notifier.

    Example:
        ```python
        @dataclass
        class MyNotifier(Notifier[MyTarget]):
            def notify(self, content: NotificationContent, target: MyTarget) -> NotifierResult:
                # Implement delivery logic
                return NotifierResult(success=True, ...)
        ```
    """

    @classmethod
    def notifier_target_class(cls) -> Type[NOTIFIER_TARGET]:
        """Get the target class type for this notifier.

        Returns:
            The target model class.
        """
        return cls.__orig_bases__[0].__args__[0]  # type: ignore

    @abstractmethod
    def notify(self, content: NotificationContent, target: NOTIFIER_TARGET) -> NotifierResult:
        """Deliver a notification to the target.

        Args:
            content (NotificationContent): The notification content to deliver.
            target (NOTIFIER_TARGET): The delivery target specification.

        Returns:
            Result indicating success or failure.
        """
        raise NotImplementedError("Please implement `notify` method")  # pragma: no cover

    @classmethod
    def parse_target(cls, target: Union[Dict[str, Any], NOTIFIER_TARGET]) -> NOTIFIER_TARGET:
        """Parse a target from a dictionary or validate an existing target.

        Args:
            target (Union[Dict[str, Any], NOTIFIER_TARGET]): Either a dictionary to parse or
                an existing target object.

        Returns:
            The parsed or validated target object.

        Raises:
            ValueError: If the target cannot be parsed.
        """
        if isinstance(target, cls.notifier_target_class()):
            return target
        elif isinstance(target, dict):
            return cls.notifier_target_class().from_dict(target)
        else:
            raise ValueError(f"Could not parse target {target} as {cls.notifier_target_class()}")
