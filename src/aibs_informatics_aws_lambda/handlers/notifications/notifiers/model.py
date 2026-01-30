"""Notifier data models.

Defines the target, content, and result models for notification delivery.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, TypeVar, Union

import marshmallow as mm
from aibs_informatics_core.models.aws.sns import SNSTopicArn
from aibs_informatics_core.models.base import (
    BooleanField,
    CustomStringField,
    EnumField,
    ListField,
    RawField,
    SchemaModel,
    StringField,
    custom_field,
)
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_core.utils.json import JSON

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


@dataclass
class NotificationContent(SchemaModel):
    """Content of a notification message.

    Attributes:
        subject: The subject line of the notification.
        message: The body content of the notification.
        content_type: The format of the message content.
    """""

    subject: str = custom_field(mm_field=StringField())
    message: str = custom_field(mm_field=StringField())
    content_type: NotificationContentType = custom_field(
        mm_field=EnumField(NotificationContentType), default=NotificationContentType.PLAIN_TEXT
    )

    @classmethod
    @mm.pre_load
    def _parse_fields(cls, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
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


@dataclass
class NotifierTarget(SchemaModel):
    """Base class for notification delivery targets.

    Subclasses define specific target types like email or SNS topics.
    """

    pass


@dataclass
class SESEmailTarget(NotifierTarget):
    """Email delivery target for SES notifications.

    Attributes:
        recipients: List of email addresses to send to.
    """

    recipients: List[EmailAddress] = custom_field(
        mm_field=ListField(CustomStringField(EmailAddress))
    )

    @classmethod
    @mm.pre_load
    def _parse_recipient_fields(cls, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        recipients = []

        for key_alias in ["recipients", "recipient", "addresses", "address"]:
            if (value_list := data.pop(key_alias, None)) is not None:
                if isinstance(value_list, str):
                    recipients.append(value_list)
                elif isinstance(value_list, list):
                    recipients.extend(value_list)
                else:
                    raise mm.ValidationError("Invalid recipient type")
        data["recipients"] = sorted(set(recipients))
        return data


@dataclass
class SNSTopicTarget(NotifierTarget):
    """SNS topic delivery target for notifications.

    Attributes:
        topic: The ARN of the SNS topic to publish to.
    """

    topic: SNSTopicArn = custom_field(mm_field=CustomStringField(SNSTopicArn))


# ----------------------------------------------------------
# Publisher Response Model
# ----------------------------------------------------------


@dataclass
class NotifierResult(SchemaModel):
    """Result of a notification delivery attempt.

    Attributes:
        target: The target the notification was sent to.
        success: Whether the delivery was successful.
        response: The raw response from the delivery service.
    """

    target: Union[dict, NotifierTarget] = custom_field(mm_field=RawField())
    success: bool = custom_field(mm_field=BooleanField())
    response: JSON = custom_field(mm_field=RawField())

    @classmethod
    @mm.post_dump
    def _serialize_target(cls, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        target = data.pop("target")
        if isinstance(target, NotifierTarget):
            target = target.to_dict()
        data["target"] = target
        return data
