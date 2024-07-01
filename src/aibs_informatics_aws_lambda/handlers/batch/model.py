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
    image: str = custom_field()
    job_definition_name: str = custom_field()
    job_queue_name: str = custom_field()
    job_name: Optional[str] = custom_field(default=None)
    command: List[str] = custom_field(default_factory=list)
    environment: Dict[str, str] = custom_field(default_factory=dict)
    job_definition_tags: Dict[str, str] = custom_field(default_factory=dict)
    resource_requirements: Union[
        List[ResourceRequirementTypeDef], ResourceRequirements
    ] = custom_field(
        default_factory=list,
        mm_field=UnionField(
            [
                (list, ListField(DictField)),
                (ResourceRequirements, ResourceRequirements.as_mm_field()),
            ]
        ),
    )
    mount_points: List[MountPointTypeDef] = custom_field(default_factory=list)
    volumes: List[VolumeTypeDef] = custom_field(default_factory=list)
    retry_strategy: Optional[RetryStrategyTypeDef] = custom_field(default=None)
    privileged: bool = custom_field(default=False)


@dataclass
class CreateDefinitionAndPrepareArgsResponse(SchemaModel):
    job_name: str = custom_field()
    job_definition_arn: Optional[str] = custom_field()
    job_queue_arn: str = custom_field()
    parameters: Dict[str, Any] = custom_field()
    container_overrides: Dict[str, Any] = custom_field()
