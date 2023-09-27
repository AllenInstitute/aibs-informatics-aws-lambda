from dataclasses import dataclass
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase, LambdaHandlerType

from aibs_informatics_core.env import ENV_BASE_KEY
from aibs_informatics_core.models.base import IntegerField, SchemaModel, custom_field
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import (
    DynamoDBRecordEventName,
)
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from aibs_informatics_aws_lambda.common.handler import DynamoDBRecord, LambdaHandler, SQSRecord


@dataclass
class NoResponse(SchemaModel):
    pass


@dataclass
class CounterRequest(SchemaModel):
    count: int = custom_field(mm_field=IntegerField())


@dataclass
class CounterResponse(SchemaModel):
    count: int = custom_field(mm_field=IntegerField())


class CounterHandler_ReqResp(LambdaHandler[CounterRequest, CounterResponse]):
    def handle(self, request: CounterRequest) -> CounterResponse:
        self.log.info(f"Hey look the count is {request.count}")
        response = CounterResponse(request.count + 1)
        self.log.info(f"Hey look the count is now {response.count}")
        return response

    @classmethod
    def deserialize_dynamodb_record(cls, record: DynamoDBRecord) -> CounterRequest:
        assert record.dynamodb and record.dynamodb.new_image
        return CounterRequest.from_dict(record.dynamodb.new_image)


class CounterHandler_ReqNoResp(LambdaHandler[CounterRequest, NoResponse]):
    def handle(self, request: CounterRequest) -> None:
        self.log.info(f"Hey look the count is {request.count}")

    @classmethod
    def should_process_sqs_record(cls, record: SQSRecord) -> bool:
        return True if record.json_body and record.json_body.get("count") != 0 else False

    @classmethod
    def should_process_dynamodb_record(cls, record: DynamoDBRecord) -> bool:
        return True if record.event_name == DynamoDBRecordEventName.INSERT else False

    @classmethod
    def deserialize_dynamodb_record(cls, record: DynamoDBRecord) -> CounterRequest:
        assert record.dynamodb and record.dynamodb.new_image
        return CounterRequest.from_dict(record.dynamodb.new_image)


class LambdaHandlerTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_vars((ENV_BASE_KEY, self.env_base))

    def get_handler(self) -> LambdaHandlerType:
        handler = LambdaHandler.get_handler()
        return handler

    def test__props__work(self):
        obj_handler = LambdaHandler()
        self.assertEqual(obj_handler.env_base, self.env_base)
        obj_handler.context

    def test__handle__method_is_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            LambdaHandler().handle({})


class CounterHandler_ReqNoResp_Tests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_vars((ENV_BASE_KEY, self.env_base))

    def get_handler(self) -> LambdaHandlerType:
        handler = CounterHandler_ReqNoResp.get_handler()
        return handler

    def test__handler__handles_valid_request_and_returns_no_response(self):
        self.assertHandles(self.get_handler(), CounterRequest(1).to_dict(), None)

    def test__handler__handles_invalid_request_and_raises_error(self):
        with self.assertRaises(Exception):
            self.assertHandles(self.get_handler(), {"counts": 1}, None)


class CounterHandler_ReqResp_Tests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_vars((ENV_BASE_KEY, self.env_base))

    def get_handler(self) -> LambdaHandlerType:
        handler = CounterHandler_ReqResp.get_handler()
        return handler

    def test__props__work(self):
        obj_handler = CounterHandler_ReqResp()
        self.assertEqual(obj_handler.env_base, self.env_base)
        obj_handler.context

    def test__handler__handles_valid_request_and_returns_response(self):
        self.assertHandles(
            self.get_handler(),
            CounterRequest(1).to_dict(),
            CounterResponse(2).to_dict(),
        )

    def test__handler__handles_invalid_request_and_raises_error(self):
        with self.assertRaises(Exception):
            self.assertHandles(self.get_handler(), {"counts": 1}, None)

    def test__sqs_handler__handles_stuffs(self):
        handler = CounterHandler_ReqResp.get_sqs_batch_handler()
        event = {
            "Records": [
                {"body": CounterRequest(1).to_json()},
                {"body": CounterRequest(0).to_json()},
            ]
        }
        self.assertHandles(handler, event, {"batchItemFailures": []})

        no_resp_handler = CounterHandler_ReqNoResp.get_sqs_batch_handler()
        self.assertHandles(no_resp_handler, event, {"batchItemFailures": []})

    def test__dynamo_handler__handles_stuffs(self):
        handler = CounterHandler_ReqResp.get_dynamodb_stream_handler()
        type_serializer = TypeSerializer()

        event = {
            "Records": [
                {
                    "eventID": "1",
                    "eventVersion": "1.0",
                    "dynamodb": {
                        "NewImage": {
                            k: type_serializer.serialize(v)
                            for k, v in CounterRequest(1).to_dict().items()
                        },
                    },
                    "eventName": "INSERT",
                },
                {
                    "eventID": "1",
                    "eventVersion": "1.0",
                    "dynamodb": {
                        "NewImage": {
                            k: type_serializer.serialize(v)
                            for k, v in CounterRequest(1).to_dict().items()
                        },
                    },
                    "eventName": "MODIFY",
                },
            ]
        }

        self.assertHandles(handler, event, {"batchItemFailures": []})

        no_resp_handler = CounterHandler_ReqNoResp.get_dynamodb_stream_handler()
        self.assertHandles(no_resp_handler, event, {"batchItemFailures": []})
