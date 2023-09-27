from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from aibs_informatics_aws_utils.batch import (
    MountPointTypeDef,
    ResourceRequirementTypeDef,
    VolumeTypeDef,
)
from aibs_informatics_core.models.aws.batch import ResourceRequirements
from aibs_informatics_core.models.base import custom_field
from aibs_informatics_core.models.base import DictField, ListField, UnionField
from aibs_informatics_core.models.base import SchemaModel


@dataclass
class CreateDefinitionAndPrepareArgsRequest(SchemaModel):
    image: str = custom_field()
    job_definition_name: str = custom_field()
    job_name: Optional[str] = custom_field(default=None)
    job_queue_name: Optional[str] = custom_field(default=None)
    command: List[str] = custom_field(default_factory=list)
    environment: Dict[str, str] = custom_field(default_factory=dict)
    job_definition_tags: Dict[str, str] = custom_field(default_factory=dict)
    resource_requirements: List[
        Union[ResourceRequirementTypeDef, ResourceRequirements]
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


@dataclass
class CreateDefinitionAndPrepareArgsResponse(SchemaModel):
    job_name: str = custom_field()
    job_definition_arn: Optional[str] = custom_field()
    job_queue_arn: str = custom_field()
    parameters: Dict[str, Any] = custom_field()
    container_overrides: Dict[str, Any] = custom_field()
