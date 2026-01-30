"""Notification data models.

Defines the request and response models for the notification system.
"""

__all__ = [
    "NotificationContentType",
    "NotificationContent",
    "NotificationRequest",
    "NotificationResponse",
]

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Union

from aibs_informatics_core.models.base import (
    EnumField,
    ListField,
    SchemaModel,
    StringField,
    UnionField,
    custom_field,
)

from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import (
    NotifierResult,
    SESEmailTarget,
    SNSTopicTarget,
)

MESSAGE_KEY_ALIASES = ["content", "body"]
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
    def _parse_fields(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        for key_alias in MESSAGE_KEY_ALIASES:
            if key_alias in data and "message" not in data:
                data["message"] = data[key_alias]
                break
        return data


@dataclass
class NotificationRequest(SchemaModel):
    """Request model for sending notifications.

    Attributes:
        content: The notification content to deliver.
        targets: List of delivery targets (SES or SNS).
    """

    content: NotificationContent = custom_field(mm_field=NotificationContent.as_mm_field())
    targets: List[Union[SESEmailTarget, SNSTopicTarget]] = custom_field(
        mm_field=ListField(
            UnionField([(_, _.as_mm_field()) for _ in [SESEmailTarget, SNSTopicTarget]]),  # type: ignore[list-item, misc]
        ),
    )


@dataclass
class NotificationResponse(SchemaModel):
    """Response model for notification delivery.

    Attributes:
        results: List of results for each notification target.
    """

    results: List[NotifierResult] = custom_field(mm_field=ListField(NotifierResult.as_mm_field()))
