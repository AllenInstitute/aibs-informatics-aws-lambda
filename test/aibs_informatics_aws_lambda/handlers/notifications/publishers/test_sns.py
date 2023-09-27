from unittest import mock

from aibs_informatics_aws_utils.sns import PublishInputRequestTypeDef
from aibs_informatics_core.exceptions import ApplicationException
from aibs_informatics_core.models.aws.sns import SNSTopicArn
from aibs_informatics_test_resources import BaseTest

from aibs_informatics_aws_lambda.common.handler import LambdaHandlerType
from aibs_informatics_aws_lambda.handlers.notifications.publishers.model import (
    PublishRequest,
    PublishRequestType,
    SNSRequest,
)
from aibs_informatics_aws_lambda.handlers.notifications.publishers.sns import (
    PublishRequest,
    SNSPublisher,
)


class SNSPublisherTest(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.mock_sns_publish_to_topic = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.notifications.publishers.sns.publish_to_topic"
        )

    def publisher(self) -> SNSPublisher:
        return SNSPublisher()

    def test__handles__simple_content(self):
        sns_topic = SNSTopicArn("arn:aws:sns:us-west-2:123456789012:MySNSTopic")
        message = "my message"

        sns_request = SNSRequest(message=message, topic=sns_topic)
        response = self.publisher().publish(
            PublishRequest(request_data=sns_request, request_type=PublishRequestType.SNS)
        )

        self.mock_sns_publish_to_topic.assert_called_once_with(
            PublishInputRequestTypeDef(Message=message, TopicArn=sns_topic)
        )

    def test__handles__failed(self):
        sns_topic = SNSTopicArn("arn:aws:sns:us-west-2:123456789012:MySNSTopic")
        message = "my message"

        self.mock_sns_publish_to_topic.side_effect = ApplicationException("error")

        sns_request = SNSRequest(message=message, topic=sns_topic)
        response = self.publisher().publish(
            PublishRequest(request_data=sns_request, request_type=PublishRequestType.SNS)
        )

        assert not response.success

    def test__should_handle_true(self):
        request_data = mock.MagicMock()
        publish_request = PublishRequest(
            request_data=request_data, request_type=PublishRequestType.SNS
        )
        assert self.publisher().should_handle(publish_request)

    def test__should_handle_false(self):
        request_data = mock.MagicMock()
        publish_request = PublishRequest(
            request_data=request_data, request_type=PublishRequestType.SES
        )
        assert not self.publisher().should_handle(publish_request)
