"""Notification data models.

Defines the request and response models for the notification system.
"""

__all__ = [
    "NotificationContentType",
    "NotificationContent",
    "NotificationRequest",
    "NotificationResponse",
]

from enum import StrEnum
from typing import Any

from aibs_informatics_core.models.base import PydanticBaseModel
from pydantic import model_validator

from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import (
    NotifierResult,
    SESEmailTarget,
    SNSTopicTarget,
)

MESSAGE_KEY_ALIASES = ["content", "body"]
"""Alternative field names accepted for the message content."""


class NotificationContentType(StrEnum):
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
    def _parse_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        for key_alias in MESSAGE_KEY_ALIASES:
            if key_alias in data and "message" not in data:
                data["message"] = data[key_alias]
                break
        return data


class NotificationRequest(PydanticBaseModel):
    """Request model for sending notifications.

    Attributes:
        content: The notification content to deliver.
        targets: List of delivery targets (SES or SNS).
    """

    content: NotificationContent
    targets: list[SESEmailTarget | SNSTopicTarget]


class NotificationResponse(PydanticBaseModel):
    """Response model for notification delivery.

    Attributes:
        results: List of results for each notification target.
    """

    results: list[NotifierResult]
