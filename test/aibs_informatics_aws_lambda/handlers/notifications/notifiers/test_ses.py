from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_test_resources import BaseTest

from aibs_informatics_aws_lambda.handlers.notifications.model import NotificationContent
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.ses import (
    SOURCE_EMAIL_ADDRESS_ENV_VAR,
    SESEmailTarget,
    SESNotifier,
)


class SESNotifierTest(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.source_address = EmailAddress("source_email@fake_address.com")
        self.set_env_vars((SOURCE_EMAIL_ADDRESS_ENV_VAR, self.source_address))
        self.mock_ses_send_simple_email = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.notifications.notifiers.ses.send_simple_email"
        )

    def test__notify__handles_simple_content(self):
        recipient = EmailAddress("test_email@fake_address.com")
        content = NotificationContent(subject="subject", message="test message")
        target = SESEmailTarget(recipients=[recipient])

        self.mock_ses_send_simple_email.return_value = {
            "MessageId": "123",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        notifier = SESNotifier()

        result = notifier.notify(content, target)

        assert result.success

        self.mock_ses_send_simple_email.assert_called_once_with(
            source=self.source_address,
            to_addresses=[recipient],
            subject=content.subject,
            body=content.message,
        )

    def test__handles_failed(self):
        recipient = EmailAddress("test_email@fake_address.com")
        content = NotificationContent(subject="subject", message="test message")
        target = SESEmailTarget(recipients=[recipient])

        self.mock_ses_send_simple_email.side_effect = AWSError("application error")

        notifier = SESNotifier()

        result = notifier.notify(content, target)

        self.mock_ses_send_simple_email.assert_called_once_with(
            source=self.source_address,
            to_addresses=[recipient],
            subject=content.subject,
            body=content.message,
        )
        assert not result.success
