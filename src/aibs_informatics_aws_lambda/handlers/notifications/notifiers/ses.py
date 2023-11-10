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
DEFAULT_SOURCE_EMAIL_ADDRESS = "marmotdev@alleninstitute.org"


@dataclass
class SESNotifier(Notifier[SESEmailTarget]):
    def notify(self, content: NotificationContent, target: SESEmailTarget) -> NotifierResult:
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
