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
    def notify(self, content: NotificationContent, target: SNSTopicTarget) -> NotifierResult:
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
