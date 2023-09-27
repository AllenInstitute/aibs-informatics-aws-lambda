import json
from dataclasses import dataclass

from aibs_informatics_aws_utils.sns import PublishInputRequestTypeDef, publish_to_topic
from aibs_informatics_aws_lambda.handlers.notifications.publishers.base import BasePublisher
from aibs_informatics_aws_lambda.handlers.notifications.publishers.model import (
    PublishRequest,
    PublishRequestType,
    PublishResponse,
)


@dataclass
class SNSPublisher(BasePublisher):
    def publish(self, request: PublishRequest) -> PublishResponse:
        try:
            sns_request = PublishInputRequestTypeDef(
                Message=request.request_data.message,
                TopicArn=request.request_data.topic,
            )
            response = publish_to_topic(sns_request)
            return PublishResponse(
                response=json.dumps(response),
                success=(200 <= response["ResponseMetadata"]["HTTPStatusCode"] < 300),
                publisher=str(self),
            )
        except Exception as e:
            return PublishResponse(response=str(e), success=False, publisher=str(self))

    def should_handle(self, request: PublishRequest) -> bool:
        if request.request_type == PublishRequestType.SNS:
            return True
        return False
