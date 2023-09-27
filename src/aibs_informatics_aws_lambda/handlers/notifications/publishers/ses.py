import json
from dataclasses import dataclass

from aibs_informatics_aws_utils.ses import send_simple_email

from aibs_informatics_aws_lambda.handlers.notifications.publishers.base import BasePublisher
from aibs_informatics_aws_lambda.handlers.notifications.publishers.constants import SOURCE_EMAIL
from aibs_informatics_aws_lambda.handlers.notifications.publishers.model import (
    PublishRequest,
    PublishRequestType,
    PublishResponse,
    SESRequest,
)


@dataclass
class SESPublisher(BasePublisher):
    def publish(self, request: PublishRequest) -> PublishResponse:
        try:
            source = SOURCE_EMAIL

            # This is a formality to nudge the typing system. In reality the `should_handle` method
            # should never allow a PublishRequest.request_data that is not an SESRequest
            assert isinstance(request.request_data, SESRequest)

            response = send_simple_email(
                source=source,
                to_addresses=[request.request_data.recipient],
                subject=request.request_data.email_subject,
                body=request.request_data.email_body,
            )
            return PublishResponse(
                response=json.dumps(response),
                success=(200 <= response["ResponseMetadata"]["HTTPStatusCode"] < 300),
                publisher=str(self),
            )
        except Exception as e:
            return PublishResponse(response=str(e), success=False, publisher=str(self))

    def should_handle(self, request: PublishRequest) -> bool:
        if request.request_type == PublishRequestType.SES:
            return True
        return False
