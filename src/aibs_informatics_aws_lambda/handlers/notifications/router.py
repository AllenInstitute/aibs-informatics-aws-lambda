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
    """Abstract Base class for notification handlers

    To use this class, create a NotificationEvent model to validate incoming events.
    Create a parse_event class to create PublishRequests.

    PublishRequests are handled with a chain-of-responsibility model. Each publisher
    checks if it can handle the PublishRequest, and if so it handles it, returning a
    PublishReponse. If it can't handle it, the request continues to the next publisher.

    If desired, you may provide a `publishers` list to specify allowed publishers

    """

    notifiers: List[Notifier] = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        if not self.notifiers:
            self.notifiers = [SESNotifier(), SNSNotifier()]

    def handle(self, request: NotificationRequest) -> NotificationResponse:
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
