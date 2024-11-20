from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.base import EnumField, SchemaModel, custom_field
from aibs_informatics_core.models.data_sync import DataSyncRequest
from aibs_informatics_core.models.demand_execution import DemandExecution

from aibs_informatics_aws_lambda.handlers.batch.model import CreateDefinitionAndPrepareArgsRequest


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


class EnvFileWriteMode(str, Enum):
    NEVER = "NEVER"
    ALWAYS = "ALWAYS"
    # TODO: revisit to see if IF_REQUIRED is really necessary or can be removed
    IF_REQUIRED = "IF_REQUIRED"


@dataclass
class DataSyncConfiguration(SchemaModel):
    intermediate_s3_path: Optional[S3Path] = custom_field(
        default=None, mm_field=S3Path.as_mm_field()
    )
    force: bool = custom_field(default=False)
    size_only: bool = custom_field(default=True)


@dataclass
class ContextManagerConfiguration(SchemaModel):
    isolate_inputs: bool = custom_field(default=False)
    env_file_write_mode: EnvFileWriteMode = custom_field(
        mm_field=EnumField(EnvFileWriteMode), default=EnvFileWriteMode.ALWAYS
    )
    # data sync configurations
    # DEPRECATED - use input_data_sync_configuration and output_data_sync_configuration instead
    intermediate_s3_path: Optional[S3Path] = custom_field(
        default=None, mm_field=S3Path.as_mm_field()
    )
    # DEPRECATED - use input_data_sync_configuration and output_data_sync_configuration instead
    force: bool = custom_field(default=False)
    # DEPRECATED - use input_data_sync_configuration and output_data_sync_configuration instead
    size_only: bool = custom_field(default=True)
    input_data_sync_configuration: DataSyncConfiguration = custom_field(
        default_factory=DataSyncConfiguration, mm_field=DataSyncConfiguration.as_mm_field()
    )
    output_data_sync_configuration: DataSyncConfiguration = custom_field(
        default_factory=DataSyncConfiguration, mm_field=DataSyncConfiguration.as_mm_field()
    )

    def __post_init__(self):
        # TEMPOARY: to support backward compatibility
        if not (self.intermediate_s3_path is None or self.force is None or self.size_only is None):
            if self.input_data_sync_configuration.intermediate_s3_path is None:
                self.input_data_sync_configuration.intermediate_s3_path = self.intermediate_s3_path
            if self.output_data_sync_configuration.intermediate_s3_path is None:
                self.output_data_sync_configuration.intermediate_s3_path = (
                    self.intermediate_s3_path
                )


@dataclass
class PrepareDemandScaffoldingRequest(SchemaModel):
    demand_execution: DemandExecution = custom_field(mm_field=DemandExecution.as_mm_field())
    file_system_configurations: DemandFileSystemConfigurations = custom_field(
        mm_field=DemandFileSystemConfigurations.as_mm_field(),
        default_factory=DemandFileSystemConfigurations,
    )
    context_manager_configuration: ContextManagerConfiguration = custom_field(
        mm_field=ContextManagerConfiguration.as_mm_field(),
        default_factory=ContextManagerConfiguration,
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
