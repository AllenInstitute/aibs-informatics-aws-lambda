"""Notification routing handler.

Provides the main Lambda handler for routing notifications
to appropriate delivery channels.
"""

from dataclasses import dataclass, field
from typing import List

from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_aws_lambda.handlers.notifications.model import (
    NotificationRequest,
    NotificationResponse,
)
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.base import Notifier
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import NotifierResult
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.ses import SESNotifier
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.sns import SNSNotifier


@dataclass  # type: ignore[misc] # mypy #5374
class NotificationRouter(LambdaHandler[NotificationRequest, NotificationResponse]):
    """Handler for routing notifications to appropriate delivery channels.

    Routes notifications to different notifiers (SES, SNS, etc.) based on
    the target specification using a chain-of-responsibility pattern.

    Each notifier attempts to parse and handle the target. If successful,
    it delivers the notification. If not, the next notifier is tried.

    Attributes:
        notifiers: List of notifier instances to try in order.

    Example:
        ```python
        handler = NotificationRouter.get_handler()
        # Or with custom notifiers
        handler = NotificationRouter(notifiers=[SESNotifier()]).get_handler()
        ```
    """

    notifiers: List[Notifier] = field(default_factory=list)

    def __post_init__(self):
        """Initialize the handler with default notifiers if none provided."""
        super().__post_init__()
        if not self.notifiers:
            self.notifiers = [SESNotifier(), SNSNotifier()]

    def handle(self, request: NotificationRequest) -> NotificationResponse:
        """Route and deliver notifications to targets.

        Attempts to deliver the notification content to each target
        using the available notifiers.

        Args:
            request (NotificationRequest): Request containing content and target specifications.

        Returns:
            Response containing results for each target.
        """
        results: List[NotifierResult] = []
        for target in request.targets:
            for notifier in self.notifiers:
                try:
                    target = notifier.parse_target(target=target)
                except Exception as e:
                    self.logger.error(f"Could not parse target {target} with {str(notifier)}: {e}")
                    continue
                else:
                    self.logger.info(f"{str(notifier)} handling target {target}")
                    notifier.notify(content=request.content, target=target)
                    break
            else:
                self.logger.error(f"No notifier could handle target {target}")
                results.append(
                    NotifierResult(
                        target=target.to_dict(),
                        success=False,
                        response="No notifier could handle target",
                    )
                )
        return NotificationResponse(results=results)
