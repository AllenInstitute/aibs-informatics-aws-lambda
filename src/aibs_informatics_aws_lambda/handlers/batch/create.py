import re
from dataclasses import dataclass
from typing import ClassVar, Optional

from aibs_informatics_aws_utils.batch import (
    BatchJobBuilder,
    build_retry_strategy,
    register_job_definition,
)
from aibs_informatics_aws_utils.ecr import resolve_image_uri
from aibs_informatics_core.collections import ValidatedStr
from aibs_informatics_core.utils.hashing import uuid_str

from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_aws_lambda.handlers.batch.model import (
    CreateDefinitionAndPrepareArgsRequest,
    CreateDefinitionAndPrepareArgsResponse,
)

REGISTRY = r"(docker\.io|public\.ecr\.aws|ghcr\.io|(?:[\d]{12}).dkr.ecr.(?:[\w-]*).amazonaws.com)"
REPO_NAME = r"([a-z0-9]+(?:[-_\.\/][a-z0-9]+)*)"
IMAGE_TAG_OR_SHA = r"(?::([\w.\-_]{1,127})|@sha256:([A-Fa-f0-9]{64}))"


# TODO: move to aibs-informatics-core once this is stable
class DockerImageUri(ValidatedStr):
    regex_pattern: ClassVar[re.Pattern] = re.compile(f"^{REGISTRY}/{REPO_NAME}{IMAGE_TAG_OR_SHA}?")

    @property
    def registry(self) -> str:
        return self.get_match_groups()[0]

    @property
    def repo_name(self) -> str:
        return self.get_match_groups()[1]

    @property
    def tag(self) -> Optional[str]:
        return self.get_match_groups()[2]

    @property
    def sha256(self) -> Optional[str]:
        return self.get_match_groups()[3]


@dataclass
class CreateDefinitionAndPrepareArgsHandler(
    LambdaHandler[CreateDefinitionAndPrepareArgsRequest, CreateDefinitionAndPrepareArgsResponse]
):
    def handle(
        self, request: CreateDefinitionAndPrepareArgsRequest
    ) -> CreateDefinitionAndPrepareArgsResponse:
        job_def_builder = BatchJobBuilder(
            image=request.image
            if DockerImageUri.is_valid(request.image)
            else resolve_image_uri(request.image),
            job_definition_name=request.job_definition_name,
            job_name=request.job_name or f"{request.job_definition_name}-{uuid_str()}",
            command=request.command,
            environment=request.environment,
            job_definition_tags=request.job_definition_tags,
            resource_requirements=request.resource_requirements,
            mount_points=request.mount_points,
            volumes=request.volumes,
            privileged=request.privileged,
        )

        response = register_job_definition(
            job_definition_name=job_def_builder.job_definition_name,
            container_properties=job_def_builder.container_properties,
            retry_strategy=request.retry_strategy or build_retry_strategy(),
            parameters=None,
            tags=job_def_builder.job_definition_tags,
        )
        job_definition_arn = response["jobDefinitionArn"]

        job_queue_name = request.job_queue_name
        if not job_queue_name:
            raise ValueError("job_queue_name must be provided")
        return CreateDefinitionAndPrepareArgsResponse(
            job_name=job_def_builder.job_name,
            job_definition_arn=job_definition_arn,
            job_queue_arn=job_queue_name,
            parameters={},
            container_overrides=job_def_builder.container_overrides__sfn,
        )


handler = CreateDefinitionAndPrepareArgsHandler.get_handler()
