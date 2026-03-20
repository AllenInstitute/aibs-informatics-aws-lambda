"""Notifier data models.

Defines the target, content, and result models for notification delivery.
"""

from enum import Enum
from typing import Any, TypeVar

from aibs_informatics_core.exceptions import ValidationError
from aibs_informatics_core.models.aws.sns import SNSTopicArn
from aibs_informatics_core.models.base import PydanticBaseModel
from aibs_informatics_core.models.email_address import EmailAddress
from pydantic import JsonValue, model_validator

NOTIFIER_TARGET = TypeVar("NOTIFIER_TARGET", bound="NotifierTarget")
"""Type variable for notifier target types."""


MESSAGE_KEY_ALIASES = ["body", "content"]
"""Alternative field names accepted for the message content."""


class NotificationContentType(str, Enum):
    """Content types for notification messages.

    Attributes:
        PLAIN_TEXT: Plain text content type.
        HTML: HTML formatted content.
        JSON: JSON structured content.
    """

    PLAIN_TEXT = "text/plain"
    HTML = "html"
    JSON = "json"


class NotificationContent(PydanticBaseModel):
    """Content of a notification message.

    Attributes:
        subject: The subject line of the notification.
        message: The body content of the notification.
        content_type: The format of the message content.
    """

    subject: str
    message: str
    content_type: NotificationContentType = NotificationContentType.PLAIN_TEXT

    @model_validator(mode="before")
    @classmethod
    def _parse_fields(cls, data: dict[str, Any]) -> dict[str, Any]:
        for key_alias in MESSAGE_KEY_ALIASES:
            if key_alias in data and "message" not in data:
                data["message"] = data[key_alias]
                break
        return data


# ----------------------------------------------------------
# Publisher Request
# ----------------------------------------------------------
class NotifierType(str, Enum):
    """Types of notification delivery channels.

    Attributes:
        SES: Amazon Simple Email Service.
        SNS: Amazon Simple Notification Service.
    """

    SES = "SES"
    SNS = "SNS"


# ----------------------------------------------------------
# Publisher Request / Response Models
# ----------------------------------------------------------


class NotifierTarget(PydanticBaseModel):
    """Base class for notification delivery targets.

    Subclasses define specific target types like email or SNS topics.
    """

    pass


class SESEmailTarget(NotifierTarget):
    """Email delivery target for SES notifications.

    Attributes:
        recipients: List of email addresses to send to.
    """

    recipients: list[EmailAddress]

    @model_validator(mode="before")
    @classmethod
    def _parse_recipient_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        recipients = []

        for key_alias in ["recipients", "recipient", "addresses", "address"]:
            if (value_list := data.pop(key_alias, None)) is not None:
                if isinstance(value_list, str):
                    recipients.append(value_list)
                elif isinstance(value_list, list):
                    recipients.extend(value_list)
                else:
                    raise ValidationError("Invalid recipient type")
        data["recipients"] = sorted(set(recipients))
        return data


class SNSTopicTarget(NotifierTarget):
    """SNS topic delivery target for notifications.

    Attributes:
        topic: The ARN of the SNS topic to publish to.
    """

    topic: SNSTopicArn


# ----------------------------------------------------------
# Publisher Response Model
# ----------------------------------------------------------


class NotifierResult(PydanticBaseModel):
    """Result of a notification delivery attempt.

    Attributes:
        target: The target the notification was sent to.
        success: Whether the delivery was successful.
        response: The raw response from the delivery service.
    """

    target: dict | NotifierTarget
    success: bool
    response: JsonValue

    @model_validator(mode="before")
    @classmethod
    def _serialize_target(cls, data: dict[str, Any]) -> dict[str, Any]:
        target = data.pop("target")
        if isinstance(target, NotifierTarget):
            target = target.to_dict()
        data["target"] = target
        return data
