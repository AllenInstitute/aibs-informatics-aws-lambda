"""Demand execution scaffolding handler.

Provides Lambda handlers for preparing demand execution scaffolding,
including file system setup and batch job configuration.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from aibs_informatics_aws_utils.batch import build_retry_strategy
from aibs_informatics_aws_utils.constants.efs import (
    EFS_SCRATCH_ACCESS_POINT_NAME,
    EFS_SCRATCH_PATH,
    EFS_SHARED_ACCESS_POINT_NAME,
    EFS_SHARED_PATH,
    EFS_TMP_ACCESS_POINT_NAME,
    EFS_TMP_PATH,
)
from aibs_informatics_aws_utils.efs import MountPointConfiguration
from aibs_informatics_core.env import EnvBase

from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_aws_lambda.handlers.demand.context_manager import (
    BatchEFSConfiguration,
    DemandExecutionContextManager,
)
from aibs_informatics_aws_lambda.handlers.demand.model import (
    CreateDefinitionAndPrepareArgsRequest,
    DemandExecutionCleanupConfigs,
    DemandExecutionSetupConfigs,
    PrepareDemandScaffoldingRequest,
    PrepareDemandScaffoldingResponse,
)


@dataclass
class PrepareDemandScaffoldingHandler(
    LambdaHandler[PrepareDemandScaffoldingRequest, PrepareDemandScaffoldingResponse]
):
    """Handler for preparing demand execution scaffolding.

    Sets up the necessary infrastructure for demand executions including:
    - EFS volume configurations for scratch, shared, and tmp storage
    - Pre-execution data sync requests for input data
    - Post-execution data sync requests for output data
    - Batch job builder configuration

    Example:
        ```python
        handler = PrepareDemandScaffoldingHandler.get_handler()
        response = handler(event, context)
        ```
    """

    def handle(self, request: PrepareDemandScaffoldingRequest) -> PrepareDemandScaffoldingResponse:
        """Prepare scaffolding for a demand execution.

        Sets up EFS configurations, creates the execution context manager,
        and generates setup and cleanup configurations.

        Args:
            request (PrepareDemandScaffoldingRequest): Request containing demand execution
                details and file system configurations.

        Returns:
            Response containing the updated demand execution and
            setup/cleanup configurations.
        """
        scratch_vol_configuration = construct_batch_efs_configuration(
            env_base=self.env_base,
            file_system=request.file_system_configurations.scratch.file_system,
            access_point=request.file_system_configurations.scratch.access_point
            if request.file_system_configurations.scratch.access_point
            else EFS_SCRATCH_ACCESS_POINT_NAME,
            container_path=request.file_system_configurations.scratch.container_path
            if request.file_system_configurations.scratch.container_path
            else f"/opt/efs{EFS_SCRATCH_PATH}",
            read_only=False,
        )

        shared_vol_configuration = construct_batch_efs_configuration(
            env_base=self.env_base,
            file_system=request.file_system_configurations.shared.file_system,
            access_point=request.file_system_configurations.shared.access_point
            if request.file_system_configurations.shared.access_point
            else EFS_SHARED_ACCESS_POINT_NAME,
            container_path=request.file_system_configurations.shared.container_path
            if request.file_system_configurations.shared.container_path
            else f"/opt/efs{EFS_SHARED_PATH}",
            read_only=True,
        )

        if request.file_system_configurations.tmp is not None:
            tmp_vol_configuration = construct_batch_efs_configuration(
                env_base=self.env_base,
                file_system=request.file_system_configurations.tmp.file_system,
                access_point=request.file_system_configurations.tmp.access_point
                if request.file_system_configurations.tmp.access_point
                else EFS_TMP_ACCESS_POINT_NAME,
                container_path=request.file_system_configurations.tmp.container_path
                if request.file_system_configurations.tmp.container_path
                else f"/opt/efs{EFS_TMP_PATH}",
                read_only=False,
            )
        else:
            tmp_vol_configuration = None

        context_manager = DemandExecutionContextManager(
            demand_execution=request.demand_execution,
            scratch_vol_configuration=scratch_vol_configuration,
            shared_vol_configuration=shared_vol_configuration,
            tmp_vol_configuration=tmp_vol_configuration,
            configuration=request.context_manager_configuration,
            env_base=self.env_base,
        )
        batch_job_builder = context_manager.batch_job_builder

        self.setup_file_system(context_manager)
        setup_configs = DemandExecutionSetupConfigs(
            data_sync_requests=[
                sync_request.from_dict(sync_request.to_dict())
                for sync_request in context_manager.pre_execution_data_sync_requests
            ],
            batch_create_request=CreateDefinitionAndPrepareArgsRequest(
                image=batch_job_builder.image,
                job_definition_name=batch_job_builder.job_definition_name,
                job_name=batch_job_builder.job_name,
                job_queue_name=context_manager.batch_job_queue_name,
                job_definition_tags=batch_job_builder.job_definition_tags,
                command=batch_job_builder.command,
                environment=batch_job_builder.environment,
                resource_requirements=batch_job_builder.resource_requirements,
                mount_points=batch_job_builder.mount_points,
                volumes=batch_job_builder.volumes,
                retry_strategy=build_retry_strategy(num_retries=5),
                privileged=batch_job_builder.privileged,
                job_role_arn=batch_job_builder.job_role_arn,
            ),
        )

        cleanup_configs = DemandExecutionCleanupConfigs(
            data_sync_requests=[
                sync_request.from_dict(sync_request.to_dict())
                for sync_request in context_manager.post_execution_data_sync_requests
            ],
            remove_data_paths_requests=context_manager.post_execution_remove_data_paths_requests,
        )

        return PrepareDemandScaffoldingResponse(
            demand_execution=context_manager.demand_execution,
            setup_configs=setup_configs,
            cleanup_configs=cleanup_configs,
        )

    def setup_file_system(self, context_manager: DemandExecutionContextManager):
        """Sets up working directory for file system

        Args:
            context_manager (DemandExecutionContextManager): context manager
        """
        working_path = context_manager.container_working_path  # noqa: F841
        # working_path.mkdir(parents=True, exist_ok=True)


def construct_batch_efs_configuration(
    env_base: EnvBase,
    container_path: Union[Path, str],
    file_system: Optional[str],
    access_point: Optional[str],
    read_only: bool = False,
) -> BatchEFSConfiguration:
    """Construct a BatchEFSConfiguration for a volume.

    Creates a mount point configuration based on the provided file system
    and access point parameters, resolving resources by tags if names
    are provided.

    Args:
        env_base (EnvBase): Environment base for resource name resolution.
        container_path (Union[Path, str]): Path where the volume will be mounted in the container.
        file_system (Optional[str]): File system ID or name (optional, resolved via tags).
        access_point (Optional[str]): Access point ID or name (optional, resolved via tags).
        read_only (bool): Whether the mount should be read-only.

    Returns:
        Configured BatchEFSConfiguration for use with AWS Batch.
    """
    mount_point_config = MountPointConfiguration.build(
        mount_point=container_path,
        access_point=access_point,
        file_system=file_system,
        access_point_tags={"env_base": env_base},
        file_system_tags={"env_base": env_base},
    )
    return BatchEFSConfiguration(mount_point_config=mount_point_config, read_only=read_only)


handler = PrepareDemandScaffoldingHandler.get_handler()
