from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase

from aibs_informatics_core.utils.hashing import uuid_str

from aibs_informatics_aws_lambda.common.api.handler import LambdaHandlerType
from aibs_informatics_aws_lambda.handlers.batch.create import CreateDefinitionAndPrepareArgsHandler
from aibs_informatics_aws_lambda.handlers.batch.model import (
    CreateDefinitionAndPrepareArgsRequest,
    CreateDefinitionAndPrepareArgsResponse,
)


class CreateDefinitionAndPrepareArgsHandlerTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_resolve_image_uri = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.batch.create.resolve_image_uri"
        )
        self.mock_register_job_definition = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.batch.create.register_job_definition"
        )
        self.mock_uuid_str = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.batch.create.uuid_str"
        )

        self.mock_resolve_image_uri.side_effect = lambda image: image
        self.mock_register_job_definition.return_value = {
            "jobDefinitionArn": "arn:aws:batch:us-east-1:123456789012:job-definition/my-job-def:1"
        }
        self.mock_uuid_str.side_effect = lambda: uuid_str("123")

    @property
    def handler(self) -> LambdaHandlerType:
        return CreateDefinitionAndPrepareArgsHandler.get_handler()

    def test__handle__success(self):
        request = CreateDefinitionAndPrepareArgsRequest(
            job_definition_name="my-job-def",
            image="my-image",
            job_queue_name="arn:aws:batch:us-east-1:123456789012:job-queue/my-job-queue",
            command=["my", "command"],
            environment={"env_key": "value"},
            job_definition_tags={"tag_key": "value"},
            resource_requirements=[{"type": "VCPU", "value": "1"}],
            mount_points=[{"containerPath": "/my/path", "sourceVolume": "my-volume"}],
            volumes=[{"name": "my-volume"}],
        )

        expected = CreateDefinitionAndPrepareArgsResponse(
            parameters={},
            job_name="my-job-def-4dfc6b14-7213-3363-8009-b23c56e3a1b1",
            job_definition_arn="arn:aws:batch:us-east-1:123456789012:job-definition/my-job-def:1",
            job_queue_arn="arn:aws:batch:us-east-1:123456789012:job-queue/my-job-queue",
            container_overrides={
                "Environment": [
                    {"Name": "AWS_REGION", "Value": "us-west-2"},
                    {"Name": "ENV_BASE", "Value": "dev-marmotdev"},
                    {"Name": "env_key", "Value": "value"},
                ],
                "ResourceRequirements": [{"Type": "VCPU", "Value": "1"}],
            },
        )

        self.assertHandles(self.handler, request.to_dict(), expected.to_dict())
