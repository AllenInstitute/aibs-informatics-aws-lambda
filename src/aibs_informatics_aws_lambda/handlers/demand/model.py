"""Demand execution data models.

Defines the request, response, and configuration models for demand
execution scaffolding and management.
"""

from enum import Enum
from typing import Any

from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.base import PydanticBaseModel
from aibs_informatics_core.models.data_sync import DataSyncRequest, PrepareBatchDataSyncRequest
from aibs_informatics_core.models.demand_execution import DemandExecution
from pydantic import Field, ValidationInfo, field_validator

from aibs_informatics_aws_lambda.handlers.batch.model import CreateDefinitionAndPrepareArgsRequest
from aibs_informatics_aws_lambda.handlers.data_sync.model import RemoveDataPathsRequest


class FileSystemConfiguration(PydanticBaseModel):
    """Configuration for an EFS file system mount.

    Attributes:
        file_system: Optional file system ID or name.
        access_point: Optional access point ID or name.
        container_path: Optional custom container mount path.
    """

    file_system: str | None = None
    access_point: str | None = None
    container_path: str | None = None


class FileSystemSelectionStrategy(str, Enum):
    """Strategy for selecting one file system from a list of candidates.

    Attributes:
        RANDOM: Uniform random selection, seeded with the demand execution ID
            so that the same execution always resolves to the same candidate.
    """

    RANDOM = "RANDOM"


class DemandFileSystemConfigurations(PydanticBaseModel):
    """Collection of file system configurations for demand execution.

    Each volume role accepts a list of candidate configurations; the scaffolding
    handler selects exactly one candidate per role (per ``selection_strategy``)
    for a given demand execution. A single (non-list) configuration is accepted
    for backwards compatibility and coerced to a one-element list.

    Attributes:
        shared: Candidate configurations for the shared/input volume (read-only).
        scratch: Candidate configurations for the scratch/working volume (read-write).
        tmp: Candidate configurations for a dedicated tmp volume. An empty list
            means no dedicated tmp volume is used.
        selection_strategy: Strategy used to select one candidate per role.
    """

    shared: list[FileSystemConfiguration] = Field(
        default_factory=lambda: [FileSystemConfiguration()]
    )
    scratch: list[FileSystemConfiguration] = Field(
        default_factory=lambda: [FileSystemConfiguration()]
    )
    tmp: list[FileSystemConfiguration] = Field(default_factory=list)
    selection_strategy: FileSystemSelectionStrategy = FileSystemSelectionStrategy.RANDOM

    @field_validator("shared", "scratch", "tmp", mode="before")
    @classmethod
    def _coerce_single_to_list(cls, value: Any) -> list[Any]:
        """Coerce a legacy single configuration (or None) into a list."""
        if value is None:
            return []
        if not isinstance(value, list):
            return [value]
        return value

    @field_validator("shared", "scratch")
    @classmethod
    def _require_non_empty(
        cls, value: list[FileSystemConfiguration], info: ValidationInfo
    ) -> list[FileSystemConfiguration]:
        if not value:
            raise ValueError(f"At least one {info.field_name} file system config is required")
        return value


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


class DataSyncConfiguration(PydanticBaseModel):
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

    temporary_request_payload_path: S3Path | None = None
    force: bool = False
    size_only: bool = True


class ContextManagerConfiguration(PydanticBaseModel):
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

    isolate_inputs: bool = True
    cleanup_inputs: bool = True
    cleanup_working_dir: bool = True
    env_file_write_mode: EnvFileWriteMode = EnvFileWriteMode.ALWAYS
    input_data_sync_configuration: DataSyncConfiguration = Field(
        default_factory=DataSyncConfiguration
    )
    output_data_sync_configuration: DataSyncConfiguration = Field(
        default_factory=DataSyncConfiguration
    )


class PrepareDemandScaffoldingRequest(PydanticBaseModel):
    """Request for preparing demand execution scaffolding.

    Attributes:
        demand_execution: The demand execution to prepare infrastructure for.
        file_system_configurations: EFS mount configurations.
        context_manager_configuration: Execution context settings.
    """

    demand_execution: DemandExecution
    file_system_configurations: DemandFileSystemConfigurations = Field(
        default_factory=DemandFileSystemConfigurations
    )
    context_manager_configuration: ContextManagerConfiguration = Field(
        default_factory=ContextManagerConfiguration
    )


class DemandExecutionSetupConfigs(PydanticBaseModel):
    """Setup configurations generated for a demand execution.

    Contains the data sync requests and batch job configuration
    needed to run the demand execution.

    Attributes:
        data_sync_requests: Requests for syncing input data.
        batch_create_request: Request to create the batch job.
    """

    # NOTE: PrepareBatchDataSyncRequest is a subclass of DataSyncRequest
    #       but it has extra fields. If DataSyncRequest is first, it will ignore
    #       the extra fields in PrepareBatchDataSyncRequest.
    #       Therefore, we need to put PrepareBatchDataSyncRequest first.
    # TODO: Consider dropping DataSyncRequest and only use PrepareBatchDataSyncRequest
    data_sync_requests: list[PrepareBatchDataSyncRequest | DataSyncRequest]
    batch_create_request: CreateDefinitionAndPrepareArgsRequest


class DemandExecutionCleanupConfigs(PydanticBaseModel):
    """Cleanup configurations generated for a demand execution.

    Contains the data sync requests and path removal requests
    to execute after the demand execution completes.

    Attributes:
        data_sync_requests: Requests for syncing output data.
        remove_data_paths_requests: Requests to remove temporary data.
    """

    # NOTE: PrepareBatchDataSyncRequest is a subclass of DataSyncRequest
    #       but it has extra fields. If DataSyncRequest is first, it will ignore
    #       the extra fields in PrepareBatchDataSyncRequest.
    #       Therefore, we need to put PrepareBatchDataSyncRequest first.
    # TODO: Consider dropping DataSyncRequest and only use PrepareBatchDataSyncRequest
    data_sync_requests: list[PrepareBatchDataSyncRequest | DataSyncRequest]
    remove_data_paths_requests: list[RemoveDataPathsRequest] = Field(default_factory=list)


class PrepareDemandScaffoldingResponse(PydanticBaseModel):
    """Response from preparing demand execution scaffolding.

    Attributes:
        demand_execution: The updated demand execution with resolved paths.
        setup_configs: Configurations for pre-execution setup.
        cleanup_configs: Configurations for post-execution cleanup.
    """

    demand_execution: DemandExecution
    setup_configs: DemandExecutionSetupConfigs
    cleanup_configs: DemandExecutionCleanupConfigs
