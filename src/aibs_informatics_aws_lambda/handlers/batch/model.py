"""AWS Batch job models.

Defines the request and response models for creating and
managing AWS Batch job definitions and submissions.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from aibs_informatics_core.models.aws.batch import ResourceRequirements
from aibs_informatics_core.models.base import (
    DictField,
    ListField,
    SchemaModel,
    UnionField,
    custom_field,
)

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
class CreateDefinitionAndPrepareArgsRequest(SchemaModel):
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

    image: str = custom_field()
    job_definition_name: str = custom_field()
    job_queue_name: str = custom_field()
    job_role_arn: Optional[str] = custom_field(default=None)
    job_name: Optional[str] = custom_field(default=None)
    command: List[str] = custom_field(default_factory=list)
    environment: Dict[str, str] = custom_field(default_factory=dict)
    job_definition_tags: Dict[str, str] = custom_field(default_factory=dict)
    resource_requirements: Union[List[ResourceRequirementTypeDef], ResourceRequirements] = (
        custom_field(
            default_factory=list,
            mm_field=UnionField(
                [
                    (list, ListField(DictField)),
                    (ResourceRequirements, ResourceRequirements.as_mm_field()),
                ]
            ),
        )
    )
    mount_points: List[MountPointTypeDef] = custom_field(default_factory=list)
    volumes: List[VolumeTypeDef] = custom_field(default_factory=list)
    retry_strategy: Optional[RetryStrategyTypeDef] = custom_field(default=None)
    privileged: bool = custom_field(default=False)


@dataclass
class CreateDefinitionAndPrepareArgsResponse(SchemaModel):
    """Response from creating a batch job definition.

    Contains the prepared arguments for submitting the batch job.

    Attributes:
        job_name: Name for the submitted job.
        job_definition_arn: ARN of the created job definition.
        job_queue_arn: ARN of the job queue.
        parameters: Job parameters for submission.
        container_overrides: Container override settings.
    """

    job_name: str = custom_field()
    job_definition_arn: Optional[str] = custom_field()
    job_queue_arn: str = custom_field()
    parameters: Dict[str, Any] = custom_field()
    container_overrides: Dict[str, Any] = custom_field()
