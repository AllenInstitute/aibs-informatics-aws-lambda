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
    ModelProtocol,
    RawField,
    SchemaModel,
    StringField,
    UnionField,
    custom_field,
)
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_core.utils.json import JSON

NOTIFICATION_EVENT = TypeVar("NOTIFICATION_EVENT", bound=ModelProtocol)


# ----------------------------------------------------------
# Publisher Request Models
# ----------------------------------------------------------
@dataclass
class SESRequest(SchemaModel):
    email_subject: str = custom_field(mm_field=StringField())
    recipient: EmailAddress = custom_field(mm_field=CustomStringField(EmailAddress))
    email_body: str = custom_field(mm_field=StringField(), default="")


@dataclass
class SNSRequest(SchemaModel):
    message: str = custom_field(mm_field=StringField())
    topic: SNSTopicArn = custom_field(mm_field=CustomStringField(SNSTopicArn))


# ----------------------------------------------------------
# Publisher Request
# the PublishRequestTypeDef is really only needed if we need to serialize the requests.
# ----------------------------------------------------------
class PublishRequestType(Enum):
    """
    Enum of all possible request types. Values are (name_string, RequestModel)
    Request Model must be serializable
    """

    SES = ("SES", SESRequest)
    SNS = ("SNS", SNSRequest)


@dataclass
class PublishRequest(SchemaModel):
    request_data: Union[SESRequest, SNSRequest] = custom_field(
        mm_field=UnionField(
            [
                (SESRequest, SESRequest.as_mm_field()),  # type: ignore
                (SNSRequest, SNSRequest.as_mm_field()),  # type: ignore
            ]
        ),
    )
    request_type: PublishRequestType = custom_field(mm_field=EnumField(PublishRequestType))

    @classmethod
    def _validate_request_data(cls, data: Dict[str, Any], **kwargs):
        pub_req_type = PublishRequestType(data["req_type"])
        request_cls = pub_req_type.value[1]
        errors = request_cls.model_schema().validate(data=data["request_data"])

        if any(errors):
            raise mm.ValidationError(
                f"Invalid request_data: {data['request_data']}, errors: {errors}"
            )

    @classmethod
    @mm.validates_schema
    def schema_validate_request_data(cls, data, **kwargs):
        cls._validate_request_data(data=data, **kwargs)

    @classmethod
    @mm.post_dump
    def post_dump_validate_request_data(cls, data, **kwargs) -> dict:
        cls._validate_request_data(data=data, **kwargs)
        return data


# ----------------------------------------------------------
# Publisher Response Model
# ----------------------------------------------------------


@dataclass
class PublishResponse(SchemaModel):
    """Base Class for Notification Responses"""

    response: JSON = custom_field(mm_field=RawField())
    success: bool = custom_field(mm_field=BooleanField())
    publisher: str = custom_field(mm_field=StringField())  # publisher.__repr__


@dataclass
class PublishResponses(SchemaModel):
    responses: List[PublishResponse] = custom_field(
        mm_field=ListField(PublishResponse.as_mm_field())
    )
