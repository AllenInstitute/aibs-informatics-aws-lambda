from unittest import mock

from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_test_resources import BaseTest

from aibs_informatics_aws_lambda.handlers.notifications.model import NotificationContent
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.sns import (
    SNSNotifier,
    SNSTopicTarget,
)


class SNSNotifierTest(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.topic_arn = "arn:aws:sns:us-east-1:123456789012:MyTopic"
        self.mock_publish_to_topic = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.notifications.notifiers.sns.publish_to_topic"
        )

    def test__notify__handles_simple_content(self):
        content = NotificationContent(subject="subject", message="test message")
        target = SNSTopicTarget(topic=self.topic_arn)

        self.mock_publish_to_topic.return_value = {
            "MessageId": "123",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        notifier = SNSNotifier()

        result = notifier.notify(content, target)

        assert result.success

        self.mock_publish_to_topic.assert_called_once_with(
            topic_arn=self.topic_arn,
            subject=content.subject,
            message=content.message,
        )

    def test__notify__handles_failure(self):
        content = NotificationContent(subject="subject", message="test message")
        target = SNSTopicTarget(topic=self.topic_arn)

        self.mock_publish_to_topic.side_effect = AWSError("application error")

        notifier = SNSNotifier()

        result = notifier.notify(content, target)

        self.mock_publish_to_topic.assert_called_once_with(
            topic_arn=self.topic_arn,
            subject=content.subject,
            message=content.message,
        )
        assert not result.success
