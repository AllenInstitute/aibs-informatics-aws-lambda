from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from aibs_informatics_core.models.base import SchemaModel, custom_field
from aibs_informatics_core.models.data_sync import DataSyncRequest
from aibs_informatics_core.models.demand_execution import DemandExecution

from aibs_informatics_aws_lambda.handlers.batch.model import CreateDefinitionAndPrepareArgsRequest
from aibs_informatics_aws_lambda.handlers.data_sync import file_system


@dataclass
class CreateDefinitionAndPrepareArgsResponse(SchemaModel):
    job_name: str = custom_field()
    job_definition_arn: Optional[str] = custom_field()
    job_queue_arn: str = custom_field()
    parameters: Dict[str, Any] = custom_field()
    container_overrides: Dict[str, Any] = custom_field()


@dataclass
class FileSystemConfiguration(SchemaModel):
    file_system: Optional[str] = None
    access_point: Optional[str] = None
    container_path: Optional[str] = None


@dataclass
class DemandFileSystemConfigurations(SchemaModel):
    shared: FileSystemConfiguration = custom_field(
        mm_field=FileSystemConfiguration.as_mm_field(), default_factory=FileSystemConfiguration
    )
    scratch: FileSystemConfiguration = custom_field(
        mm_field=FileSystemConfiguration.as_mm_field(), default_factory=FileSystemConfiguration
    )
    tmp: Optional[FileSystemConfiguration] = custom_field(
        mm_field=FileSystemConfiguration.as_mm_field(), default=None
    )


@dataclass
class PrepareDemandScaffoldingRequest(SchemaModel):
    demand_execution: DemandExecution = custom_field(mm_field=DemandExecution.as_mm_field())
    file_system_configurations: DemandFileSystemConfigurations = custom_field(
        mm_field=DemandFileSystemConfigurations.as_mm_field(),
        default_factory=DemandFileSystemConfigurations,
    )


@dataclass
class DemandExecutionSetupConfigs(SchemaModel):
    data_sync_requests: List[DataSyncRequest]
    batch_create_request: CreateDefinitionAndPrepareArgsRequest


@dataclass
class DemandExecutionCleanupConfigs(SchemaModel):
    data_sync_requests: List[DataSyncRequest]


@dataclass
class PrepareDemandScaffoldingResponse(SchemaModel):
    demand_execution: DemandExecution = custom_field(mm_field=DemandExecution.as_mm_field())
    setup_configs: DemandExecutionSetupConfigs = custom_field(
        mm_field=DemandExecutionSetupConfigs.as_mm_field()
    )
    cleanup_configs: DemandExecutionCleanupConfigs = custom_field(
        mm_field=DemandExecutionCleanupConfigs.as_mm_field()
    )
