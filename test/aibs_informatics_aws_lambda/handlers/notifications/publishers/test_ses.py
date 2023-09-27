from unittest import mock

from aibs_informatics_core.exceptions import ApplicationException
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_test_resources import BaseTest

from aibs_informatics_aws_lambda.handlers.notifications.publishers.model import (
    PublishRequest,
    PublishRequestType,
    SESRequest,
)
from aibs_informatics_aws_lambda.handlers.notifications.publishers.ses import SESPublisher


class SESPublisherTest(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.source_address = EmailAddress("source_email@fake_address.com")
        self.mock_source_email = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.notifications.publishers.ses.SOURCE_EMAIL",
            new=self.source_address,
        )
        self.mock_ses_send_simple_email = self.create_patch(
            # "gcs_aws_utils.ses.send_simple_email"
            "aibs_informatics_aws_lambda.handlers.notifications.publishers.ses.send_simple_email"
        )

    def publisher(self) -> SESPublisher:
        return SESPublisher()

    def test__handles_simple_content(self):
        recipient = EmailAddress("test_email@fake_address.com")
        message = "test message"

        self.mock_ses_send_simple_email.return_value = {
            "MessageId": "123",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        ses_request = SESRequest(email_subject=message, recipient=recipient)
        response = self.publisher().publish(
            PublishRequest(request_data=ses_request, request_type=PublishRequestType.SES)
        )
        self.mock_ses_send_simple_email.assert_called_once_with(
            source=self.source_address, to_addresses=[recipient], subject=message, body=""
        )
        assert response.success

    def test__handles_failed(self):
        recipient = EmailAddress("test_email@fake_address.com")
        message = "test message"

        self.mock_ses_send_simple_email.side_effect = ApplicationException("application error")

        ses_request = SESRequest(email_subject=message, recipient=recipient)
        response = self.publisher().publish(
            PublishRequest(request_data=ses_request, request_type=PublishRequestType.SES)
        )
        self.mock_ses_send_simple_email.assert_called_once_with(
            source=self.source_address, to_addresses=[recipient], subject=message, body=""
        )
        assert not response.success

    def test__should_handle_true(self):
        ses_request = mock.MagicMock()
        publish_request = PublishRequest(
            request_data=ses_request, request_type=PublishRequestType.SES
        )
        assert self.publisher().should_handle(publish_request)

    def test__should_handle_false(self):
        request_data = mock.MagicMock()
        publish_request = PublishRequest(
            request_data=request_data, request_type=PublishRequestType.SNS
        )
        assert not self.publisher().should_handle(publish_request)
