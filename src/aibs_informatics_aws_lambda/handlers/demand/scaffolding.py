from dataclasses import dataclass
from pathlib import Path
from random import choice
from typing import Any, Dict, List, Optional, Union, cast

from aibs_informatics_aws_utils.batch import build_retry_strategy
from aibs_informatics_aws_utils.constants.efs import (
    EFS_SCRATCH_ACCESS_POINT_NAME,
    EFS_SCRATCH_PATH,
    EFS_SHARED_ACCESS_POINT_NAME,
    EFS_SHARED_PATH,
    EFS_TMP_ACCESS_POINT_NAME,
    EFS_TMP_PATH,
)
from aibs_informatics_aws_utils.efs import (
    MountPointConfiguration,
    get_efs_access_point,
    get_efs_file_system,
)
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
    FileSystemConfiguration,
    FileSystemSelectionStrategy,
    PrepareDemandScaffoldingRequest,
    PrepareDemandScaffoldingResponse,
)


@dataclass
class PrepareDemandScaffoldingHandler(
    LambdaHandler[PrepareDemandScaffoldingRequest, PrepareDemandScaffoldingResponse]
):
    def handle(self, request: PrepareDemandScaffoldingRequest) -> PrepareDemandScaffoldingResponse:
        if request.file_system_configurations.scratch is None:
            raise ValueError("Scratch file system configuration is required")

        scratch_fs_config = select_file_system(
            request.file_system_configurations.scratch,
            request.file_system_configurations.selection_strategy,
        )

        scratch_vol_configuration = construct_batch_efs_configuration(
            env_base=self.env_base,
            file_system=scratch_fs_config.file_system,
            access_point=scratch_fs_config.access_point,
            container_path=scratch_fs_config.container_path
            if scratch_fs_config.container_path
            else f"/opt/efs{EFS_SCRATCH_PATH}",
            read_only=False,
        )

        shared_fs_config = select_file_system(
            request.file_system_configurations.shared,
            request.file_system_configurations.selection_strategy,
        )

        shared_vol_configuration = construct_batch_efs_configuration(
            env_base=self.env_base,
            file_system=shared_fs_config.file_system,
            access_point=shared_fs_config.access_point,
            container_path=shared_fs_config.container_path
            if shared_fs_config.container_path
            else f"/opt/efs{EFS_SHARED_PATH}",
            read_only=True,
        )

        if request.file_system_configurations.tmp:
            tmp_fs_config = select_file_system(
                request.file_system_configurations.tmp,
                request.file_system_configurations.selection_strategy,
            )

            tmp_vol_configuration = construct_batch_efs_configuration(
                env_base=self.env_base,
                file_system=tmp_fs_config.file_system,
                access_point=tmp_fs_config.access_point,
                container_path=tmp_fs_config.container_path
                if tmp_fs_config.container_path
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
        working_path = context_manager.container_working_path
        # working_path.mkdir(parents=True, exist_ok=True)


def select_file_system(
    file_system_configurations: List[FileSystemConfiguration],
    selection_strategy: FileSystemSelectionStrategy,
) -> FileSystemConfiguration:
    # Edge cases
    if len(file_system_configurations) == 0:
        raise ValueError("No file system configurations provided")
    elif len(file_system_configurations) == 1:
        return file_system_configurations[0]

    # Main logic

    if selection_strategy == FileSystemSelectionStrategy.RANDOM:
        # Randomly select a file system configuration from the list
        return file_system_configurations[choice(range(len(file_system_configurations)))]
    elif selection_strategy == FileSystemSelectionStrategy.LEAST_UTILIZED:
        # Select the file system configuration with the least amount of storage used

        fs_id_to_description: Dict[str, Dict[str, Any]] = {}

        # Iterate through the list of provided file system configurations and resolve the associated file system ID
        for fsconfig in file_system_configurations:
            if fsconfig.file_system:
                # If the file system ID is provided and not already in the dictionary, fetch its details
                if fsconfig.file_system not in fs_id_to_description:
                    file_system = get_efs_file_system(file_system_id=fsconfig.file_system)
                    fs_id_to_description[fsconfig.file_system] = file_system
            elif fsconfig.access_point:
                # If only an access point is provided, retrieve the associated file system ID
                access_point = get_efs_access_point(access_point_id=fsconfig.access_point)
                fsconfig.file_system = access_point.get("FileSystemId")

                # If the resolved file system ID is valid and not already stored, fetch its details
                if fsconfig.file_system and fsconfig.file_system not in fs_id_to_description:
                    file_system = get_efs_file_system(file_system_id=fsconfig.file_system)
                    fs_id_to_description[fsconfig.file_system] = file_system

        # Filter out configurations that do not have an associated file system and sort them
        # by the amount of storage currently used (ascending order, selecting the least used)
        sorted_configs = sorted(
            filter(lambda _: _.file_system is not None, file_system_configurations),
            key=lambda fsconfig: fs_id_to_description[cast(str, fsconfig.file_system)][
                "SizeInBytes"
            ]["Value"],
        )

        # Return the file system configuration with the least amount of storage used
        return sorted_configs[0]

    else:
        raise ValueError(f"Unknown selection strategy: {selection_strategy}")


def construct_batch_efs_configuration(
    env_base: EnvBase,
    container_path: Union[Path, str],
    file_system: Optional[str],
    access_point: Optional[str],
    read_only: bool = False,
) -> BatchEFSConfiguration:
    mount_point_config = MountPointConfiguration.build(
        mount_point=container_path,
        access_point=access_point,
        file_system=file_system,
        access_point_tags={"env_base": env_base},
        file_system_tags={"env_base": env_base},
    )
    return BatchEFSConfiguration(mount_point_config=mount_point_config, read_only=read_only)


handler = PrepareDemandScaffoldingHandler.get_handler()
