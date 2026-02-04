"""SNS topic notification delivery.

Provides notification delivery via Amazon Simple Notification Service (SNS).
"""

import json
from dataclasses import dataclass

from aibs_informatics_aws_utils.sns import publish_to_topic

from aibs_informatics_aws_lambda.handlers.notifications.model import NotificationContent
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.base import Notifier
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import (
    NotifierResult,
    SNSTopicTarget,
)


@dataclass
class SNSNotifier(Notifier[SNSTopicTarget]):
    """Notifier implementation for publishing to Amazon SNS topics.

    Publishes notification content to SNS topics for fanout to
    subscribers (email, SMS, HTTP endpoints, etc.).

    Example:
        ```python
        notifier = SNSNotifier()
        result = notifier.notify(
            content=NotificationContent(subject="Alert", message="Critical event"),
            target=SNSTopicTarget(topic="arn:aws:sns:us-east-1:123456789012:my-topic")
        )
        ```
    """

    def notify(self, content: NotificationContent, target: SNSTopicTarget) -> NotifierResult:
        """Publish a notification to an SNS topic.

        Args:
            content (NotificationContent): The notification content including subject and message.
            target (SNSTopicTarget): The SNS topic target containing the topic ARN.

        Returns:
            Result indicating success or failure with response details.
        """
        try:
            response = publish_to_topic(
                message=content.message,
                subject=content.subject,
                topic_arn=target.topic,
            )
            return NotifierResult(
                response=json.dumps(response),
                success=(200 <= response["ResponseMetadata"]["HTTPStatusCode"] < 300),
                target=target.to_dict(),
            )
        except Exception as e:
            return NotifierResult(
                target=target.to_dict(),
                success=False,
                response=str(e),
            )
