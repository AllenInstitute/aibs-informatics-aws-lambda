from dataclasses import dataclass
from typing import List, Union

from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import DynamoDBRecord
from aibs_informatics_core.models.aws.sns import SNSTopicArn
from aibs_informatics_core.models.db import GWODemandRegistryEntry
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_aws_lambda.common.notification_handler import NotificationLambdaHandler
from aibs_informatics_aws_lambda.handlers.notifications.publishers.model import (
    PublishRequest,
    PublishRequestType,
    SESRequest,
    SNSRequest,
)


@dataclass
class DemandStatusNotificationHandler(NotificationLambdaHandler[GWODemandRegistryEntry]):
    """
    Notifies GWODemandRegistryEntry entry.notify_list recipients if entry.status changes to
    entry.notify_on
    """

    def parse_event(
        self,
        event: GWODemandRegistryEntry,
    ) -> List[PublishRequest]:
        gwo_entry = event

        message = (
            f"OCS demand [{gwo_entry.demand_workflow_name}] (Demand ID {gwo_entry.demand_id}) "
            f"updated with status: [{gwo_entry.status}]"
        )
        detailed_message = (
            f"Here are the details of the [{gwo_entry.status}] "
            f"OCS demand [{gwo_entry.demand_workflow_name}]:\n"
            f"Detailed status message: [{gwo_entry.message}]\n"
            f"Complete OCS demand-registry entry:\n{gwo_entry.to_json()}"
        )
        publish_requests: List[PublishRequest] = []
        for recipient in gwo_entry.notify_list:
            request_data: Union[SNSRequest, SESRequest]
            if SNSTopicArn.is_valid(recipient):
                request_data = SNSRequest(topic=SNSTopicArn(recipient), message=message)
                request_type = PublishRequestType.SNS
            elif EmailAddress.is_valid(recipient):
                request_data = SESRequest(
                    recipient=EmailAddress(recipient),
                    email_subject=message,
                    email_body=detailed_message,
                )
                request_type = PublishRequestType.SES
            else:
                raise ValueError(
                    f"Unknown recipient type {type(recipient)}, "
                    "should be EmailAddress or SNSTopicArn"
                )
            publish_requests.append(
                PublishRequest(request_type=request_type, request_data=request_data)
            )
        return publish_requests

    @classmethod
    def get_entry_from_record(cls, record: DynamoDBRecord) -> GWODemandRegistryEntry:
        if not record.dynamodb or not record.dynamodb.new_image:
            raise ValueError(
                f"Cannot deserialize DynamoDB record {record} to {cls.get_request_cls()}. "
                f"This stream event does not contain new image."
            )
        return GWODemandRegistryEntry.from_dict(record.dynamodb.new_image)

    @classmethod
    def deserialize_dynamodb_record(cls, record: DynamoDBRecord) -> GWODemandRegistryEntry:
        return cls.get_entry_from_record(record)


handler = DemandStatusNotificationHandler.get_handler()
dynamodb_stream_handler = DemandStatusNotificationHandler.get_dynamodb_stream_handler()
