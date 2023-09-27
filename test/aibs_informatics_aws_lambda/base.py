from abc import abstractmethod
from test.base import BaseTest
from typing import Callable, Optional, Type

from aibs_informatics_core.env import ENV_BASE_KEY
from aibs_informatics_core.utils.json import JSON
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.typing.lambda_client_context import LambdaClientContext
from aws_lambda_powertools.utilities.typing.lambda_client_context_mobile_client import (
    LambdaClientContextMobileClient,
)
from aws_lambda_powertools.utilities.typing.lambda_cognito_identity import LambdaCognitoIdentity

from aibs_informatics_aws_lambda.common.api_handler import LambdaEvent


class MockLambdaContext(LambdaContext):
    def __init__(self, function_name: str):
        self._function_name = function_name
        self._function_version = "1.0"
        self._invoked_function_arn = (
            f"arn:aws:lambda:us-west-2:123456789012:function:{function_name}"
        )
        self._memory_limit_in_mb = 500
        self._aws_request_id = "12345678-1234-1234-1234-123456789012"
        self._log_group_name = f"/test/{function_name}"
        self._log_stream_name = f"{self.aws_request_id}"

        identity = LambdaCognitoIdentity()
        identity._cognito_identity_id = "id"
        identity._cognito_identity_pool_id = "pool_id"

        client_context = LambdaClientContext()
        client_context._client = LambdaClientContextMobileClient()

        self._identity = LambdaCognitoIdentity()
        self._client_context = LambdaClientContext()


LambdaHandlerType = Callable[[LambdaEvent, LambdaContext], Optional[JSON]]


class LambdaHandlerTestCase(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_vars((ENV_BASE_KEY, self.env_base))
        self.set_env_vars(("REGION", "us-west-2"))
        self.set_env_vars(("ACCOUNT", "123456789012"))

    @property
    @abstractmethod
    def handler(self) -> LambdaHandlerType:
        pass

    @property
    def context(self) -> LambdaContext:
        return MockLambdaContext(self.__class__.__name__)

    def assertHandles(
        self,
        handler: LambdaHandlerType,
        event: LambdaEvent,
        response: Optional[JSON] = None,
        context: Optional[LambdaContext] = None,
    ):
        actual = handler(event, context or self.context)
        if response is None:
            self.assertIsNone(actual)
        elif isinstance(response, dict) and isinstance(actual, dict):
            self.assertDictEqual(response, actual)
        elif isinstance(response, list) and isinstance(actual, list):
            self.assertListEqual(response, actual)
        else:
            self.assertEqual(response, actual)

    def assertLambdaRaises(
        self,
        handler: LambdaHandlerType,
        event: LambdaEvent,
        exception: Type[Exception],
        context: Optional[LambdaContext] = None,
    ):
        with self.assertRaises(exception):
            handler(event, context or self.context)
