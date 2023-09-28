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


class NotificationContentType(str, Enum):
    PLAIN_TEXT = "text/plain"
    HTML = "html"
    JSON = "json"


@dataclass
class NotificationContent(SchemaModel):
    """Base Class for Notification Requests"""

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
    content: NotificationContent = custom_field(mm_field=NotificationContent.as_mm_field())
    targets: List[Union[SESEmailTarget, SNSTopicTarget]] = custom_field(
        mm_field=ListField(
            UnionField([(_, _.as_mm_field()) for _ in [SESEmailTarget, SNSTopicTarget]]),  # type: ignore[list-item]
        ),
    )


@dataclass
class NotificationResponse(SchemaModel):
    results: List[NotifierResult] = custom_field(mm_field=ListField(NotifierResult.as_mm_field()))
