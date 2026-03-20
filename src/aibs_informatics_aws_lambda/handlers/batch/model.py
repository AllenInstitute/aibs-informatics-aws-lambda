"""AWS Batch job models.

Defines the request and response models for creating and
managing AWS Batch job definitions and submissions.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aibs_informatics_core.models.aws.batch import ResourceRequirements
from aibs_informatics_core.models.base import (
    PydanticBaseModel,
)
from pydantic import Field

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_batch.type_defs import (
        MountPointTypeDef,
        ResourceRequirementTypeDef,
        RetryStrategyTypeDef,
        VolumeTypeDef,
    )
else:
    MountPointTypeDef = dict
    ResourceRequirementTypeDef = dict
    RetryStrategyTypeDef = dict
    VolumeTypeDef = dict


@dataclass
class CreateDefinitionAndPrepareArgsRequest(PydanticBaseModel):
    """Request for creating a batch job definition and preparing submission args.

    Attributes:
        image: Docker image URI for the job.
        job_definition_name: Name for the job definition.
        job_queue_name: Name of the job queue to submit to.
        job_role_arn: Optional IAM role ARN for the job.
        job_name: Optional name for the submitted job.
        command: Command to run in the container.
        environment: Environment variables for the container.
        job_definition_tags: Tags to apply to the job definition.
        resource_requirements: CPU, memory, and GPU requirements.
        mount_points: EFS/volume mount points.
        volumes: Volume definitions.
        retry_strategy: Job retry configuration.
        privileged: Whether to run in privileged mode.
    """

    image: str
    job_definition_name: str
    job_queue_name: str
    job_role_arn: str | None = None
    job_name: str | None = None
    command: list[str] = Field(default_factory=list)
    environment: dict[str, str] = Field(default_factory=dict)
    job_definition_tags: dict[str, str] = Field(default_factory=dict)
    resource_requirements: list[ResourceRequirementTypeDef] | ResourceRequirements = Field(
        default_factory=list,
    )
    mount_points: list[MountPointTypeDef] = Field(default_factory=list)
    volumes: list[VolumeTypeDef] = Field(default_factory=list)
    retry_strategy: RetryStrategyTypeDef | None = None
    privileged: bool = Field(default=False)


@dataclass
class CreateDefinitionAndPrepareArgsResponse(PydanticBaseModel):
    """Response from creating a batch job definition.

    Contains the prepared arguments for submitting the batch job.

    Attributes:
        job_name: Name for the submitted job.
        job_definition_arn: ARN of the created job definition.
        job_queue_arn: ARN of the job queue.
        parameters: Job parameters for submission.
        container_overrides: Container override settings.
    """

    job_name: str
    job_definition_arn: str | None
    job_queue_arn: str
    parameters: dict[str, Any]
    container_overrides: dict[str, Any]
