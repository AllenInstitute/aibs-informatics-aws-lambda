"""SES email notification delivery.

Provides notification delivery via Amazon Simple Email Service (SES).
"""

import json
from dataclasses import dataclass

from aibs_informatics_aws_utils.ses import send_simple_email
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_core.utils.os_operations import get_env_var

from aibs_informatics_aws_lambda.handlers.notifications.notifiers.base import Notifier
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import (
    NotificationContent,
    NotifierResult,
    SESEmailTarget,
)

SOURCE_EMAIL_ADDRESS_ENV_VAR = "SOURCE_EMAIL_ADDRESS"
"""Environment variable name for the source email address."""

DEFAULT_SOURCE_EMAIL_ADDRESS = "marmotdev@alleninstitute.org"
"""Default source email address if environment variable is not set."""


@dataclass
class SESNotifier(Notifier[SESEmailTarget]):
    """Notifier implementation for sending emails via Amazon SES.

    Sends notification content as email to specified recipients using
    Amazon Simple Email Service.

    The source email address is configured via the SOURCE_EMAIL_ADDRESS
    environment variable, with a fallback to the default address.

    Example:
        ```python
        notifier = SESNotifier()
        result = notifier.notify(
            content=NotificationContent(subject="Hello", message="World"),
            target=SESEmailTarget(recipients=["user@example.com"])
        )
        ```
    """

    def notify(self, content: NotificationContent, target: SESEmailTarget) -> NotifierResult:
        """Send an email notification via SES.

        Args:
            content (NotificationContent): The notification content including subject and message.
            target (SESEmailTarget): The email target containing recipient addresses.

        Returns:
            Result indicating success or failure with response details.
        """
        try:
            source = EmailAddress(
                get_env_var(
                    SOURCE_EMAIL_ADDRESS_ENV_VAR, default_value=DEFAULT_SOURCE_EMAIL_ADDRESS
                )
            )

            response = send_simple_email(
                source=source,
                to_addresses=target.recipients,
                subject=content.subject,
                body=content.message,
                # TODO: in future we may want to support html emails
            )
            return NotifierResult(
                response=json.dumps(response),
                success=(200 <= response["ResponseMetadata"]["HTTPStatusCode"] < 300),
                target=target.to_dict(),
            )
        except Exception as e:
            return NotifierResult(
                response=str(e),
                success=False,
                target=target.to_dict(),
            )
