from dataclasses import dataclass
from pathlib import Path
from random import choice, seed
from typing import Any, Dict, List, Optional, Union, cast

from aibs_informatics_aws_utils.batch import build_retry_strategy
from aibs_informatics_aws_utils.constants.efs import (
    EFS_SCRATCH_PATH,
    EFS_SHARED_PATH,
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
    """
    Lambda handler for preparing the scaffolding required for demand execution.

    This handler is responsible for setting up the file system configurations and creating the
    execution context for a demand request. It selects the appropriate file systems for scratch,
    shared, and (optionally) temporary volumes based on a specified selection strategy (e.g.,
    RANDOM or LEAST_UTILIZED) and configures the associated EFS mount points. The handler then
    constructs the necessary Batch job setup and cleanup configurations by initializing a
    DemandExecutionContextManager.

    Key responsibilities include:
      - Selecting the file system configuration for each volume (scratch, shared, and tmp) using
        a strategy and an optional seed (derived from the demand execution ID) for reproducibility.
      - Constructing EFS volume configurations by calling `construct_batch_efs_configuration`,
        ensuring that default container paths are used when none are specified.
      - Initializing a DemandExecutionContextManager to manage pre- and post-execution data sync
        requests, Batch job definitions, and cleanup steps.
      - Setting up the file system, such as creating the working directory.

    The handler expects a request of type `PrepareDemandScaffoldingRequest` and returns a response of
    type `PrepareDemandScaffoldingResponse` containing demand execution details along with the associated
    setup and cleanup configurations.
    """

    def handle(self, request: PrepareDemandScaffoldingRequest) -> PrepareDemandScaffoldingResponse:
        """
        Process the PrepareDemandScaffoldingRequest to configure the demand execution environment.

        This method executes the following steps:
          1. Selects the file system configurations for the scratch and shared volumes using the
             provided selection strategy and the execution ID as a seed. If a temporary file system
             configuration is provided, it is similarly selected.
          2. Constructs EFS volume configurations for each selected file system by invoking
             `construct_batch_efs_configuration`. Default container paths are assigned if not specified.
          3. Instantiates a DemandExecutionContextManager using the configured volume settings and
             context manager configuration from the request.
          4. Calls `setup_file_system` to perform necessary file system preparations (e.g., creating
             the working directory).
          5. Prepares the Batch job definition, including data sync requests and resource settings, and
             compiles the setup and cleanup configurations.
          6. Returns a `PrepareDemandScaffoldingResponse` containing the demand execution details,
             Batch job setup configurations, and cleanup configurations.

        Parameters:
            request (PrepareDemandScaffoldingRequest): The request object containing:
                - File system configurations for scratch, shared, and optionally temporary volumes.
                - Demand execution metadata (including an execution ID for seeding randomness).
                - Context manager configuration details.

        Returns:
            PrepareDemandScaffoldingResponse: A response object that includes:
                - The demand execution details.
                - The setup configurations required for the Batch job.
                - The cleanup configurations for post-execution tasks.

        Raises:
            ValueError: If required file system configurations are missing or invalid.
        """
        scratch_fs_config = select_file_system(
            request.file_system_configurations.scratch,
            selection_strategy=request.file_system_configurations.selection_strategy,
            seed_number=request.demand_execution.execution_id,
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
            selection_strategy=request.file_system_configurations.selection_strategy,
            seed_number=request.demand_execution.execution_id,
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
                selection_strategy=request.file_system_configurations.selection_strategy,
                seed_number=request.demand_execution.execution_id,
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
    seed_number: Optional[Union[str, int]] = None,
) -> FileSystemConfiguration:
    """
    Select a file system configuration from a list based on the specified selection strategy.

    This function evaluates the provided list of file system configurations and returns one configuration
    according to the given strategy. Two strategies are supported:

    1. RANDOM:
       - Randomly selects one configuration from the list.
       - If a seed_number is provided, it seeds the random number generator for reproducible results.

    2. LEAST_UTILIZED:
       - Chooses the configuration whose associated file system has the least storage used.
       - For each configuration, it retrieves the file system details (using the file system ID or by
         resolving the access point to a file system ID) and sorts them by the "SizeInBytes" value (i.e.,
         the amount of storage used), returning the configuration with the smallest usage.

    Parameters:
        file_system_configurations (List[FileSystemConfiguration]):
            A list of file system configurations. Each configuration must specify either a file system ID
            or an access point ID to resolve the file system details.
        selection_strategy (FileSystemSelectionStrategy):
            The strategy used to select a file system configuration. Supported values are:
                - RANDOM: Selects a configuration at random.
                - LEAST_UTILIZED: Selects the configuration with the lowest storage utilization.
        seed_number (Optional[Union[str, int]], optional):
            An optional seed for the random number generator when using the RANDOM strategy. Defaults to None.

    Returns:
        FileSystemConfiguration: The selected file system configuration based on the provided strategy.

    Raises:
        ValueError: If no file system configurations are provided.
        ValueError: If a configuration lacks both a file system ID and an access point ID.
        ValueError: If an unknown selection strategy is specified.
    """

    # Edge cases
    if len(file_system_configurations) == 0:
        raise ValueError("No file system configurations provided")
    elif len(file_system_configurations) == 1:
        return file_system_configurations[0]

    # Main logic

    if selection_strategy == FileSystemSelectionStrategy.RANDOM:
        # Randomly select a file system configuration from the list
        if seed_number is not None:
            seed(seed_number)
        return choice(file_system_configurations)
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
            else:
                raise ValueError(
                    "Neither file system ID nor access point ID provided for file system configuration"
                )

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
        raise ValueError(f"Unknown selection strategy: {selection_strategy}")  # pragma: no cover


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
