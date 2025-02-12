from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union

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
    """
    Configuration for how data sync should be run.

    Attributes:
        temporary_request_payload_path (Optional[S3Path]):
            The path to the temporary request payload. This is useful if many requests will
            be generated and the payloads are too large to be passed in the state machine
            context object.
        force (bool):
            If True, the data will be synced even if it already exists and satisfies
            the checksum or size check.
        size_only (bool):
            If True, only the size of the data will be checked when determining if the data
            should be synced. If False, the checksum of the data will also be checked.

    """

    temporary_request_payload_path: Optional[S3Path] = custom_field(
        default=None, mm_field=S3Path.as_mm_field()
    )
    force: bool = custom_field(default=False)
    size_only: bool = custom_field(default=True)


@dataclass
class ContextManagerConfiguration(SchemaModel):
    """
    Configuration for managing the context in which a demand execution runs.

    Attributes:
        isolate_inputs (bool):
            If True, input data will be written to working directory instead of the shared
            scratch directory. This is useful if:
            - you want to ensure that input data is not modified by other processes,
            - can be modified by the demand execution,
            - and can be cleaned up immediately after completion
                (RO shared scratch data is not cleaned up).
        cleanup_inputs (bool):
            If True, input data will be cleaned up after execution. Note that this may
            not work as expected if isolate_inputs is False, as inputs are typically
            mounted as read-only. This will be clear when defining infrastructure code.
        cleanup_working_dir (bool):
            If True, the working directory will be cleaned up after execution. This is useful if
            you want to ensure that no data is left behind in the working directory.
        env_file_write_mode (EnvFileWriteMode):
            Determines when environment files should be written instead of being added to the list
            of env variables in batch job definition. Options are NEVER, ALWAYS, and IF_REQUIRED.
            IF_REQUIRED is experimental and attempts to write env files only if the env variables
            exceed a certain length.
        input_data_sync_configuration (DataSyncConfiguration):
            Configuration for syncing input data. The force flag and size_only flag are used to
            determine how the data is synced. The temporary_request_payload_path should be used
            if many requests will be generated and the payloads are too large to be passed
            in the state machine context object.
        output_data_sync_configuration (DataSyncConfiguration):
            Configuration for syncing output data. The force flag and size_only flag are used to
            determine how the data is synced. The temporary_request_payload_path should be used
            if many requests will be generated and the payloads are too large to be passed
            in the state machine context object.
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
    demand_execution: DemandExecution = custom_field(mm_field=DemandExecution.as_mm_field())
    setup_configs: DemandExecutionSetupConfigs = custom_field(
        mm_field=DemandExecutionSetupConfigs.as_mm_field()
    )
    cleanup_configs: DemandExecutionCleanupConfigs = custom_field(
        mm_field=DemandExecutionCleanupConfigs.as_mm_field()
    )
