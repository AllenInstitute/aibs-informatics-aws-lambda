from test.base import BaseTest
from typing import Optional

from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.json import JSON
from aws_lambda_powertools.utilities.typing import LambdaContext

from aibs_informatics_aws_lambda.common.handler import LambdaEvent
from aibs_informatics_aws_lambda.common.models import DefaultLambdaContext, LambdaHandlerRequest
from aibs_informatics_aws_lambda.main import (
    AWS_LAMBDA_EVENT_PAYLOAD_KEY,
    AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY,
    AWS_LAMBDA_FUNCTION_HANDLER_KEY,
    handle,
    handle_cli,
)


def mock_handler(event: LambdaEvent, context: LambdaContext) -> Optional[JSON]:
    if isinstance(event, dict):
        if event.get("fail", False):
            raise ValueError("something went wrong")
        if event.get("response", False):
            return event
    return None


class TestMain(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_vars((EnvBase.ENV_BASE_KEY, self.env_base))
        self.set_env_vars(("REGION", "us-west-2"))
        self.set_env_vars(("ACCOUNT_ID", "123456789012"))
        self.create_patch(
            "aibs_informatics_aws_lambda.common.models.get_account_id"
        ).return_value = "123456789012"
        self.create_patch("aibs_informatics_aws_lambda.main.upload_json")

    def test__handle__succeeds(self):
        event = {"response": False}
        response = handle(
            LambdaHandlerRequest(mock_handler, event).to_dict(), DefaultLambdaContext()
        )

    def test__handle_cli__succeeds__resolves_args_from_env__no_response(self):
        self.set_env_vars(
            (AWS_LAMBDA_FUNCTION_HANDLER_KEY, "test.aibs_informatics_aws_lambda.test_main.mock_handler")
        )
        self.set_env_vars((AWS_LAMBDA_EVENT_PAYLOAD_KEY, "{}"))
        handle_cli([])

    def test__handle_cli__succeeds__resolves_args_from_arguments__no_response(self):
        handle_cli(
            ["--handler", "test.aibs_informatics_aws_lambda.test_main.mock_handler", "--payload", "{}"]
        )

    def test__handle_cli__fails__no_args_or_env_vars(self):
        with self.assertRaises(ValueError):
            handle_cli([])
        with self.assertRaises(ValueError):
            handle_cli(["--handler", "test.aibs_informatics_aws_lambda.test_main.mock_handler"])

    def test__handle_cli__succeeds__writes_response_to_file(self):
        handler_name = "test.aibs_informatics_aws_lambda.test_main.mock_handler"
        payload = '{"response": true}'
        response_location = self.tmp_path() / "response.json"
        handle_cli(
            [
                "--handler",
                handler_name,
                "--payload",
                payload,
                "--response-location",
                str(response_location),
            ]
        )
        self.assertEqual(response_location.read_text(), payload)

    def test__handle_cli__succeeds__writes_response_to_s3(self):
        handler_name = "test.aibs_informatics_aws_lambda.test_main.mock_handler"
        payload = '{"response": true}'
        response_location = "s3://bucket/response.json"
        handle_cli(
            [
                "--handler",
                handler_name,
                "--payload",
                payload,
                "--response-location",
                str(response_location),
            ]
        )

    def test__handle_cli__succeeds__writes_empty_response_to_file(self):
        handler_name = "test.aibs_informatics_aws_lambda.test_main.mock_handler"
        payload = '{"response": false}'
        response_location = self.tmp_path() / "response.json"
        handle_cli(
            [
                "--handler",
                handler_name,
                "--payload",
                payload,
                "--response-location",
                str(response_location),
            ]
        )
        self.assertEqual(response_location.read_text(), "{}")

    def test__handle_cli__fails__cannot_write_to_dir(self):
        handler_name = "test.aibs_informatics_aws_lambda.test_main.mock_handler"
        payload = '{"response": false}'
        response_location = self.tmp_path()
        with self.assertRaises(ValueError):
            handle_cli(
                [
                    "--handler",
                    handler_name,
                    "--payload",
                    payload,
                    "--response-location",
                    str(response_location),
                ]
            )
