from dataclasses import dataclass
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase, LambdaHandlerType

from aibs_informatics_core.env import ENV_BASE_KEY
from aibs_informatics_core.models.base import SchemaModel, IntegerField, custom_field
from aibs_informatics_aws_lambda.common.handler import LambdaHandler


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


class CounterHandler_ReqNoResp(LambdaHandler[CounterRequest, NoResponse]):
    def handle(self, request: CounterRequest) -> None:
        self.log.info(f"Hey look the count is {request.count}")


class GCSLambdaHandlerTests(LambdaHandlerTestCase):
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
        self.assertHandles(handler, {"Records": []}, {"batchItemFailures": []})
