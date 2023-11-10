from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Dict, List, TypeVar, Union

import marshmallow as mm
from aibs_informatics_core.collections import StrEnum
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


MESSAGE_KEY_ALIASES = ["body", "content"]


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
    SES = "SES"
    SNS = "SNS"


# ----------------------------------------------------------
# Publisher Request / Response Models
# ----------------------------------------------------------


@dataclass
class NotifierTarget(SchemaModel):
    pass


@dataclass
class SESEmailTarget(NotifierTarget):
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
    topic: SNSTopicArn = custom_field(mm_field=CustomStringField(SNSTopicArn))


# ----------------------------------------------------------
# Publisher Response Model
# ----------------------------------------------------------


@dataclass
class NotifierResult(SchemaModel):
    """Base Class for Notification Responses"""

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
