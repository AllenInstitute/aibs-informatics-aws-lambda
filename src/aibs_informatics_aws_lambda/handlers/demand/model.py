"""Demand execution data models.

Defines the request, response, and configuration models for demand
execution scaffolding and management.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union

from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.base import (
    EnumField,
    ListField,
    SchemaModel,
    UnionField,
    custom_field,
)
from aibs_informatics_core.models.data_sync import DataSyncRequest, PrepareBatchDataSyncRequest
from aibs_informatics_core.models.demand_execution import DemandExecution

from aibs_informatics_aws_lambda.handlers.batch.model import CreateDefinitionAndPrepareArgsRequest
from aibs_informatics_aws_lambda.handlers.data_sync.model import RemoveDataPathsRequest


@dataclass
class FileSystemConfiguration(SchemaModel):
    """Configuration for an EFS file system mount.

    Attributes:
        file_system: Optional file system ID or name.
        access_point: Optional access point ID or name.
        container_path: Optional custom container mount path.
    """

    file_system: Optional[str] = None
    access_point: Optional[str] = None
    container_path: Optional[str] = None


@dataclass
class DemandFileSystemConfigurations(SchemaModel):
    """Collection of file system configurations for demand execution.

    Attributes:
        shared: Configuration for the shared/input volume (read-only).
        scratch: Configuration for the scratch/working volume (read-write).
        tmp: Optional configuration for a dedicated tmp volume.
    """

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
    """Modes for writing environment files in batch jobs.

    Controls whether environment variables are written to a file
    on EFS or passed directly to the container.

    Attributes:
        NEVER: Never write env file, always pass variables directly.
        ALWAYS: Always write env file to avoid variable size limits.
        IF_REQUIRED: Write env file only if variables exceed size threshold.
    """

    NEVER = "NEVER"
    ALWAYS = "ALWAYS"
    # TODO: revisit to see if IF_REQUIRED is really necessary or can be removed
    IF_REQUIRED = "IF_REQUIRED"


@dataclass
class DataSyncConfiguration(SchemaModel):
    """Configuration for data synchronization behavior.

    Controls how data is synced between S3 and EFS for demand executions.

    Attributes:
        temporary_request_payload_path: Optional S3 path for storing large
            request payloads that exceed state machine limits.
        force: If True, sync data even if it already exists and passes
            checksum/size validation.
        size_only: If True, only check file sizes when validating sync.
            If False, also verify checksums.
    """

    temporary_request_payload_path: Optional[S3Path] = custom_field(
        default=None, mm_field=S3Path.as_mm_field()
    )
    force: bool = custom_field(default=False)
    size_only: bool = custom_field(default=True)


@dataclass
class ContextManagerConfiguration(SchemaModel):
    """Configuration for demand execution context management.

    Controls behavior of input/output handling, cleanup, and environment
    variable management.

    Attributes:
        isolate_inputs: If True, copy inputs to working directory instead
            of using shared scratch. Useful for mutable inputs.
        cleanup_inputs: If True, remove input data after execution.
        cleanup_working_dir: If True, remove working directory after execution.
        env_file_write_mode: How to handle environment variable files.
        input_data_sync_configuration: Configuration for input data sync.
        output_data_sync_configuration: Configuration for output data sync.
    """

    isolate_inputs: bool = custom_field(default=True)
    cleanup_inputs: bool = custom_field(default=True)
    cleanup_working_dir: bool = custom_field(default=True)
    env_file_write_mode: EnvFileWriteMode = custom_field(
        mm_field=EnumField(EnvFileWriteMode), default=EnvFileWriteMode.ALWAYS
    )
    input_data_sync_configuration: DataSyncConfiguration = custom_field(
        default_factory=DataSyncConfiguration, mm_field=DataSyncConfiguration.as_mm_field()
    )
    output_data_sync_configuration: DataSyncConfiguration = custom_field(
        default_factory=DataSyncConfiguration, mm_field=DataSyncConfiguration.as_mm_field()
    )


@dataclass
class PrepareDemandScaffoldingRequest(SchemaModel):
    """Request for preparing demand execution scaffolding.

    Attributes:
        demand_execution: The demand execution to prepare infrastructure for.
        file_system_configurations: EFS mount configurations.
        context_manager_configuration: Execution context settings.
    """

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
    """Setup configurations generated for a demand execution.

    Contains the data sync requests and batch job configuration
    needed to run the demand execution.

    Attributes:
        data_sync_requests: Requests for syncing input data.
        batch_create_request: Request to create the batch job.
    """

    data_sync_requests: List[Union[DataSyncRequest, PrepareBatchDataSyncRequest]] = custom_field(
        mm_field=UnionField(
            [
                # NOTE: PrepareBatchDataSyncRequest is a subclass of DataSyncRequest
                #       but it has extra fields. If DataSyncRequest is first, it will ignore
                #       the extra fields in PrepareBatchDataSyncRequest.
                #       Therefore, we need to put PrepareBatchDataSyncRequest first.
                # TODO: Consider dropping DataSyncRequest and only use PrepareBatchDataSyncRequest
                (list, ListField(PrepareBatchDataSyncRequest.as_mm_field())),
                (list, ListField(DataSyncRequest.as_mm_field())),
            ]
        )
    )
    batch_create_request: CreateDefinitionAndPrepareArgsRequest = custom_field(
        mm_field=CreateDefinitionAndPrepareArgsRequest.as_mm_field()
    )


@dataclass
class DemandExecutionCleanupConfigs(SchemaModel):
    """Cleanup configurations generated for a demand execution.

    Contains the data sync requests and path removal requests
    to execute after the demand execution completes.

    Attributes:
        data_sync_requests: Requests for syncing output data.
        remove_data_paths_requests: Requests to remove temporary data.
    """

    data_sync_requests: List[Union[DataSyncRequest, PrepareBatchDataSyncRequest]] = custom_field(
        mm_field=UnionField(
            [
                # NOTE: PrepareBatchDataSyncRequest is a subclass of DataSyncRequest
                #       but it has extra fields. If DataSyncRequest is first, it will ignore
                #       the extra fields in PrepareBatchDataSyncRequest.
                #       Therefore, we need to put PrepareBatchDataSyncRequest first.
                # TODO: Consider dropping DataSyncRequest and only use PrepareBatchDataSyncRequest
                (list, ListField(PrepareBatchDataSyncRequest.as_mm_field())),
                (list, ListField(DataSyncRequest.as_mm_field())),
            ]
        )
    )
    remove_data_paths_requests: List[RemoveDataPathsRequest] = custom_field(
        mm_field=ListField(RemoveDataPathsRequest.as_mm_field()), default_factory=list
    )


@dataclass
class PrepareDemandScaffoldingResponse(SchemaModel):
    """Response from preparing demand execution scaffolding.

    Attributes:
        demand_execution: The updated demand execution with resolved paths.
        setup_configs: Configurations for pre-execution setup.
        cleanup_configs: Configurations for post-execution cleanup.
    """

    demand_execution: DemandExecution = custom_field(mm_field=DemandExecution.as_mm_field())
    setup_configs: DemandExecutionSetupConfigs = custom_field(
        mm_field=DemandExecutionSetupConfigs.as_mm_field()
    )
    cleanup_configs: DemandExecutionCleanupConfigs = custom_field(
        mm_field=DemandExecutionCleanupConfigs.as_mm_field()
    )
