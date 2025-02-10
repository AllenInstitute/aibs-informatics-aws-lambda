import logging
import re
import sys
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from aibs_informatics_aws_utils.batch import (
    BatchJobBuilder,
    to_mount_point,
    to_resource_requirements,
    to_volume,
)
from aibs_informatics_aws_utils.constants.efs import (
    EFS_SCRATCH_ACCESS_POINT_NAME,
    EFS_SCRATCH_PATH,
    EFS_SHARED_ACCESS_POINT_NAME,
    EFS_SHARED_PATH,
)
from aibs_informatics_aws_utils.efs import (
    MountPointConfiguration,
    get_efs_access_point,
    get_efs_file_system,
    get_local_path,
)
from aibs_informatics_aws_utils.efs.paths import get_efs_path
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.models.data_sync import PrepareBatchDataSyncRequest
from aibs_informatics_core.models.demand_execution import DemandExecution
from aibs_informatics_core.models.demand_execution.resolvables import Resolvable, Uploadable
from aibs_informatics_core.utils.hashing import sha256_hexdigest
from aibs_informatics_core.utils.os_operations import write_env_file
from aibs_informatics_core.utils.units import BYTES_PER_GIBIBYTE

from aibs_informatics_aws_lambda.handlers.data_sync.model import RemoveDataPathsRequest
from aibs_informatics_aws_lambda.handlers.demand.model import (
    ContextManagerConfiguration,
    EnvFileWriteMode,
)

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_batch.type_defs import (
        EFSVolumeConfigurationTypeDef,
        MountPointTypeDef,
        VolumeTypeDef,
    )
else:
    MountPointTypeDef = dict
    VolumeTypeDef = dict
    EFSVolumeConfigurationTypeDef = dict


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class BatchEFSConfiguration:
    mount_point_config: MountPointConfiguration
    read_only: bool = False
    mount_point: MountPointTypeDef = field(init=False)
    volume: VolumeTypeDef = field(init=False)

    def __post_init__(self) -> None:
        volume_name = "-".join(
            [
                f'{self.mount_point_config.file_system.get("Name", self.mount_point_config.file_system["FileSystemId"])}',
                f'{str(self.mount_path).strip("/").replace("/","-")}-vol',
            ]
        )

        efs_volume_configuration: EFSVolumeConfigurationTypeDef = {
            "fileSystemId": self.mount_point_config.file_system["FileSystemId"],
            "rootDirectory": "/",
        }
        if self.mount_point_config.access_point:
            efs_volume_configuration["transitEncryption"] = "ENABLED"
            efs_volume_configuration["authorizationConfig"] = {
                "accessPointId": self.mount_point_config.access_point["AccessPointId"],  # type: ignore
                "iam": "DISABLED",
            }
        self.mount_point = to_mount_point(
            self.mount_point_config.mount_point.as_posix(), self.read_only, volume_name
        )
        self.volume = to_volume(None, volume_name, efs_volume_configuration)

    @property
    def mount_path(self) -> Path:
        return self.mount_point_config.mount_point

    @classmethod
    def build(cls, access_point: str, mount_path: Union[Path, str], read_only: bool = False):
        mount_point_config = MountPointConfiguration.build(
            mount_point=mount_path,
            access_point=access_point,
        )
        return BatchEFSConfiguration(mount_point_config=mount_point_config, read_only=read_only)


@dataclass
class DemandExecutionContextManager:
    demand_execution: DemandExecution
    scratch_vol_configuration: BatchEFSConfiguration
    shared_vol_configuration: BatchEFSConfiguration
    tmp_vol_configuration: Optional[BatchEFSConfiguration] = None
    configuration: ContextManagerConfiguration = field(default_factory=ContextManagerConfiguration)
    env_base: EnvBase = field(default_factory=EnvBase.from_env)

    def __post_init__(self):
        self._batch_job_builder = None
        logging.info(f"Creating DemandExecutionContextManager {self}")
        self.demand_execution = update_demand_execution_parameter_inputs(
            demand_execution=self.demand_execution,
            container_shared_path=self.container_shared_path,
            container_working_path=self.container_working_path,
            isolate_inputs=self.configuration.isolate_inputs,
        )
        self.demand_execution = update_demand_execution_parameter_outputs(
            self.demand_execution, self.container_working_path
        )

    @property
    def container_working_path(self) -> Path:
        """Returns the container path for the working data path for the demand execution

        Example:
            /opt/efs/scratch/{EXECUTION_ID}


        Returns:
            Path: container path for working data path directory
        """
        return self.scratch_vol_configuration.mount_point_config.as_mounted_path(
            self.demand_execution.execution_id
        )

    @property
    def container_tmp_path(self) -> Path:
        """Returns the container path for the tmp volume

        Example:
            /opt/efs/scratch/tmp

        Returns:
            Path: container path for tmp volume
        """
        if self.tmp_vol_configuration:
            return self.tmp_vol_configuration.mount_point_config.mount_point
        return self.scratch_vol_configuration.mount_point_config.as_mounted_path("tmp")

    @property
    def container_shared_path(self) -> Path:
        """Returns the container path for the shared volume

        Example:
            /opt/efs/shared

        Returns:
            Path: container path for shared volume
        """
        return self.shared_vol_configuration.mount_point_config.mount_point

    @property
    def efs_working_path(self) -> EFSPath:
        """Returns the global EFS path for working data path for the demand execution

        Example:
            efs://fs-12345678:/scratch/{EXECUTION_ID}

        Returns:
            EFSPath: EFS URI for working data path directory
        """
        return get_efs_path(self.container_working_path, mount_points=self.efs_mount_points)

    @property
    def efs_tmp_path(self) -> EFSPath:
        """Returns the global EFS path for tmp data path (scratch)

        Example:
            efs://fs-12345678:/scratch/tmp

        Returns:
            EFSPath: EFS URI for tmp data path directory
        """
        return get_efs_path(self.container_tmp_path, mount_points=self.efs_mount_points)

    @property
    def efs_shared_path(self) -> EFSPath:
        """Returns the global EFS path for shared (inputs) data path

        Example:
            efs://fs-12345678:/shared

        Returns:
            EFSPath: EFS URI for shared data path directory
        """
        return get_efs_path(self.container_shared_path, mount_points=self.efs_mount_points)

    @property
    def efs_mount_points(self) -> List[MountPointConfiguration]:
        """Returns a list of mount points for the EFS volumes used by the aws batch job

        Returns:
            List[MountPointConfiguration]: list of mount point configurations
        """
        mpcs = [
            self.scratch_vol_configuration.mount_point_config,
            self.shared_vol_configuration.mount_point_config,
        ]
        if self.tmp_vol_configuration:
            mpcs.append(self.tmp_vol_configuration.mount_point_config)
        return mpcs

    @property
    def batch_job_builder(self) -> BatchJobBuilder:
        if not self._batch_job_builder:
            self._batch_job_builder = generate_batch_job_builder(
                demand_execution=self.demand_execution,
                working_path=self.efs_working_path,
                tmp_path=self.efs_tmp_path,
                scratch_mount_point=self.scratch_vol_configuration.mount_point_config,
                shared_mount_point=self.shared_vol_configuration.mount_point_config,
                tmp_mount_point=self.tmp_vol_configuration.mount_point_config
                if self.tmp_vol_configuration
                else None,
                env_base=self.env_base,
                env_file_write_mode=self.configuration.env_file_write_mode,
            )
        return self._batch_job_builder

    @property
    def batch_job_queue_name(self) -> str:
        return get_batch_job_queue_name(self.demand_execution)

    @property
    def pre_execution_data_sync_requests(self) -> List[PrepareBatchDataSyncRequest]:
        requests = []
        temporary_request_payload_root = (
            self.configuration.input_data_sync_configuration.temporary_request_payload_path
        )
        for i, param in enumerate(
            self.demand_execution.execution_parameters.downloadable_job_param_inputs
        ):
            temporary_request_payload_path = None
            if temporary_request_payload_root:
                temporary_request_payload_path = temporary_request_payload_root / f"input_{i}/"
            requests.append(
                PrepareBatchDataSyncRequest(
                    source_path=S3URI(param.remote_value),
                    destination_path=get_efs_path(
                        local_path=Path(param.value),
                        raise_if_unresolved=True,
                        mount_points=self.efs_mount_points,
                    ),
                    retain_source_data=True,
                    require_lock=True,
                    batch_size_bytes_limit=75 * BYTES_PER_GIBIBYTE,  # 75 GiB max
                    temporary_request_payload_path=temporary_request_payload_path,
                    size_only=self.configuration.input_data_sync_configuration.size_only,
                    force=self.configuration.input_data_sync_configuration.force,
                )
            )
        logger.info(
            f"Generated {len(requests)} data sync requests for pre-execution data sync: {requests}"
        )
        return requests

    @property
    def post_execution_data_sync_requests(self) -> List[PrepareBatchDataSyncRequest]:
        requests = []
        temporary_request_payload_root = (
            self.configuration.output_data_sync_configuration.temporary_request_payload_path
        )
        for i, param in enumerate(
            self.demand_execution.execution_parameters.uploadable_job_param_outputs
        ):
            temporary_request_payload_path = None
            if temporary_request_payload_root:
                temporary_request_payload_path = temporary_request_payload_root / f"output_{i}/"
            requests.append(
                PrepareBatchDataSyncRequest(
                    source_path=get_efs_path(Path(param.value), True, self.efs_mount_points),
                    destination_path=S3URI(param.remote_value),
                    retain_source_data=False,
                    require_lock=False,
                    batch_size_bytes_limit=75 * BYTES_PER_GIBIBYTE,  # 75 GiB max
                    temporary_request_payload_path=temporary_request_payload_path,
                    size_only=self.configuration.output_data_sync_configuration.size_only,
                    force=self.configuration.output_data_sync_configuration.force,
                )
            )
        logger.info(
            f"Generated {len(requests)} data sync requests for post-execution data sync: {requests}"
        )
        return requests

    @property
    def post_execution_remove_data_paths_requests(self) -> List[RemoveDataPathsRequest]:
        """Generates remove data paths requests for post-execution data sync

        Returns:
            List[RemoveDataPathsRequest]: list of remove data paths requests
        """
        requests = []
        if self.configuration.cleanup_inputs:
            input_paths = []
            for param in self.demand_execution.execution_parameters.downloadable_job_param_inputs:
                input_paths.append(get_efs_path(Path(param.value), True, self.efs_mount_points))
            requests.append(RemoveDataPathsRequest(paths=input_paths))

        if self.configuration.cleanup_working_dir:
            requests.append(RemoveDataPathsRequest(paths=[self.efs_working_path]))
        return requests

    @classmethod
    def from_demand_execution(
        cls,
        demand_execution: DemandExecution,
        env_base: EnvBase,
        configuration: Optional[ContextManagerConfiguration] = None,
    ):
        vol_configuration = get_batch_efs_configuration(
            env_base=env_base,
            container_path=f"/opt/efs{EFS_SCRATCH_PATH}",
            access_point_name=EFS_SCRATCH_ACCESS_POINT_NAME,
            read_only=False,
        )
        shared_vol_configuration = get_batch_efs_configuration(
            env_base=env_base,
            container_path=f"/opt/efs{EFS_SHARED_PATH}",
            access_point_name=EFS_SHARED_ACCESS_POINT_NAME,
            read_only=True,
        )
        tmp_vol_configuration = None

        logger.info(f"Using following efs configuration: {vol_configuration}")
        return DemandExecutionContextManager(
            demand_execution=demand_execution,
            scratch_vol_configuration=vol_configuration,
            shared_vol_configuration=shared_vol_configuration,
            tmp_vol_configuration=tmp_vol_configuration,
            configuration=configuration or ContextManagerConfiguration(),
            env_base=env_base,
        )


# ------------------------------------------------------------------
#  Private Functions
# ------------------------------------------------------------------


def update_demand_execution_parameter_inputs(
    demand_execution: DemandExecution,
    container_shared_path: Path,
    container_working_path: Path,
    isolate_inputs: bool = False,
) -> DemandExecution:
    """Modifies demand execution input destinations with the location of the volume configuration

    This updates the input destinations to a deterministic location under the volume configuration
    specified. This ensures that inputs shared between jobs can used the same cached results.

    The structure of the path for any input param is comprised of:
        - volume's mount_path (where on container this volume is mounted)
        - a sha256 hash value of the parmeter's remote value

    PATTERN: {MOUNT_PATH}/{SHA256_HASH(PARAM_REMOTE_VALUE)}

    Example:
        Given volume:
            - mount path (/opt/efs/shared)
        Given execution parameter inputs:
            - X (s3://bucket/prefix/A)
            - Y (s3://bucket/prefix/A)
            - Z (s3://bucket/prefix/B)
        Output:
           - X -> /opt/efs/shared/abcdef...AAAA
           - Y -> /opt/efs/shared/abcdef...AAAA
           - Z -> /opt/efs/shared/abcdef...BBBB

    Args:
        demand_execution (DemandExecution): Demand execution object to modify (copied)
        vol_configuration (BatchEFSConfiguration): volume configuration
        isolate_inputs (bool): flag to determine if inputs should be isolated

    Returns:
        DemandExecution: a demand execution with modified execution parameter inputs
    """

    demand_execution = demand_execution.copy()
    execution_params = demand_execution.execution_parameters
    updated_params = {}
    for param in execution_params.downloadable_job_param_inputs:
        if isolate_inputs:
            local = container_working_path / param.value
            logger.info(f"Isolating input {param.name} from shared volume. Local path: {local}")
        else:
            local = container_shared_path / sha256_hexdigest(param.remote_value)
            logger.info(f"Using shared volume for input {param.name}. Local path: {local}")

        new_resolvable = Resolvable(local=local.as_posix(), remote=param.remote_value)
        updated_params[param.name] = new_resolvable

    execution_params.update_params(**updated_params)
    return demand_execution


def update_demand_execution_parameter_outputs(
    demand_execution: DemandExecution,
    container_working_path: Path,
) -> DemandExecution:
    """Modifies the demand execution output's local paths relative to the container working path

    This updates the output destinations to their absolute location under the container working
    path.

    PATTERN: {CONTAINER_WORKING_PATH}/{PARAM_VALUE}

    Example:
        Given volume:
            - mount path working dir (/opt/efs/scratch/UUID)
        Given execution parameter outputs:
            - X (s3://bucket/prefix/A)
            - Y (s3://bucket/prefix/B)
            - Z (s3://bucket/prefix/C)
        Output (local value):
           - X -> /opt/efs/scratch/UUID/X
           - Y -> /opt/efs/scratch/UUID/Y
           - Z -> /opt/efs/scratch/UUID/Z

    Args:
        demand_execution (DemandExecution): Demand execution object to modify (copied)
        vol_configuration (BatchEFSConfiguration): volume configuration

    Returns:
        DemandExecution: a demand execution with modified execution parameter inputs
    """

    demand_execution = demand_execution.copy()
    execution_params = demand_execution.execution_parameters
    updated_params = {
        param.name: Uploadable(
            local=(container_working_path / param.value).as_posix(),
            remote=param.remote_value,
        )
        for param in execution_params.uploadable_job_param_outputs
    }
    execution_params.update_params(**updated_params)
    return demand_execution


def get_batch_efs_configuration(
    env_base: EnvBase,
    container_path: str,
    access_point_name: str,
    file_system_name: Optional[str] = None,
    read_only: bool = False,
) -> BatchEFSConfiguration:
    # TODO: add support for file_system_name (learn how to resolve file system name)
    if file_system_name:
        file_system_name = env_base.get_resource_name(file_system_name)
        file_system = get_efs_file_system(name=file_system_name, tags={"env_base": env_base})
        file_system_id = file_system["FileSystemId"]
        logger.info(
            f"Using file system {file_system_id} with name {file_system_name}. "
            f"Will search for access point {access_point_name}."
        )
    else:
        logger.info(
            f"No file system name provided. "
            f"Will search for access point {access_point_name} directly."
        )
        file_system_id = None

    access_point = get_efs_access_point(
        file_system_id=file_system_id,
        access_point_name=access_point_name,
        access_point_tags={"env_base": env_base},
    )
    logger.info(f"Using access point {access_point_name}")
    return BatchEFSConfiguration.build(
        access_point=access_point["AccessPointId"],
        mount_path=container_path,
        read_only=read_only,
    )


def generate_batch_job_builder(
    demand_execution: DemandExecution,
    env_base: EnvBase,
    working_path: EFSPath,
    tmp_path: EFSPath,
    scratch_mount_point: MountPointConfiguration,
    shared_mount_point: MountPointConfiguration,
    tmp_mount_point: Optional[MountPointConfiguration] = None,
    env_file_write_mode: EnvFileWriteMode = EnvFileWriteMode.ALWAYS,
) -> BatchJobBuilder:
    logger.info(f"Constructing BatchJobBuilder instance")

    demand_execution = demand_execution.copy()
    efs_mount_points = [scratch_mount_point, shared_mount_point]
    if tmp_mount_point is not None:
        efs_mount_points.append(tmp_mount_point)
    logger.info(f"Resolving local paths of working dir = {working_path} and tmp dir = {tmp_path}")
    container_working_path = get_local_path(working_path, mount_points=efs_mount_points)
    container_tmp_path = get_local_path(tmp_path, mount_points=efs_mount_points)

    logger.info(f"Setting container working directory = {container_working_path}")

    EXECUTION_ID_VAR = "EXECUTION_ID"
    WORKING_DIR_VAR = "WORKING_DIR"
    TMPDIR_VAR = "TMPDIR"

    environment: Dict[str, str] = {
        EXECUTION_ID_VAR: demand_execution.execution_id,
        WORKING_DIR_VAR: f"{container_working_path}",
        TMPDIR_VAR: f"{container_tmp_path}",
    }

    for job_param in demand_execution.execution_parameters.job_params:
        job_param.update_environment(environment)

    logger.info(f"Environment updated with {len(environment)} environment variables.")

    pre_commands = [
        f"mkdir -p ${{{WORKING_DIR_VAR}}}".split(" "),
        f"mkdir -p ${{{TMPDIR_VAR}}}".split(" "),
        f"cd ${{{WORKING_DIR_VAR}}}".split(" "),
    ]

    logger.info(f"Initial pre-commands: {pre_commands}")

    command = deepcopy(demand_execution.execution_parameters.command)
    if not command:
        logger.warning("No command specified, trying to resolve from manifest")
        # TODO: add logic to resolve default command from manifest
        raise ValueError("Must specify command for demand execution")

    logger.info(f"Command extracted from demand execution: {command}")

    # ------------------------------------------------------------------
    ##  Environment File Conditional Configuration Logic
    #
    # This step tries to write the environment variables to a file that will be mounted
    # to the container at runtime. This only works if the local machine that runs this code
    # has access to EFS file system.
    #
    logger.info(
        "Attempting to create environment file for environment variables. "
        f"Using {working_path} directory to write file."
    )
    # Here we define three path variables. They point to env file from the perspective of:
    #   1. The container path
    #   2. An EFS URI (pointing to location on EFS file system)
    #   3. The (future) path on the local machine running this code.
    container_environment_file = container_working_path / ".demand.env"
    efs_environment_file_uri = get_efs_path(
        container_environment_file, mount_points=efs_mount_points
    )
    local_environment_file = get_local_path(efs_environment_file_uri, raise_if_unmounted=False)

    # If the local environment file is not None, then the file is writable from this local machine
    # We will now write a portion of environment variables to files that can be written.
    if local_environment_file is None or env_file_write_mode == EnvFileWriteMode.NEVER:
        # If the environment file cannot be written to, then the environment variables are
        # passed directly to the container. This is a fallback option and will fail if the
        # environment variables are too long.
        if local_environment_file is None:
            reason = f"Could not write environment variables to file {efs_environment_file_uri}."
        else:
            reason = "Environment file write mode set to NEVER."

        logger.warning(
            f"{reason} Environment variables will be passed directly to the container. "
            "THIS MAY CAUSE THE CONTAINER TO FAIL IF THE ENVIRONMENT VARIABLES "
            "ARE LONGER THAN 8192 CHARACTERS!!!"
        )

    else:
        if env_file_write_mode == EnvFileWriteMode.IF_REQUIRED:
            env_size = sum([sys.getsizeof(k) + sys.getsizeof(v) for k, v in environment.items()])

            if env_size > 8192 * 0.9:
                logger.info(
                    f"Environment variables are too large to pass directly to container (> 90% of 8192). "
                    f"Writing environment variables to file {efs_environment_file_uri}."
                )
                confirm_write = True
            else:
                confirm_write = False
        elif env_file_write_mode == EnvFileWriteMode.ALWAYS:
            logger.info(f"Writing environment variables to file {efs_environment_file_uri}.")
            confirm_write = True

        if confirm_write:
            # Steps for writing environment variables to file:
            #   1. Identify all environment variables that are not referenced in the command
            #       if not referenced, then add to environment file.
            #   2. Write environment file
            #   3. Add environment file to command
            ENVIRONMENT_FILE_VAR = "_ENVIRONMENT_FILE"

            # Step 1:, split environment variables based on reference are referenced in the command
            writable_environment = environment.copy()
            required_environment: Dict[str, str] = {}
            for arg in command + [_ for c in pre_commands for _ in c]:
                for match in re.findall(r"\$\{?([\w]+)\}?", arg):
                    if match in writable_environment:
                        required_environment[match] = writable_environment.pop(match)

            # Add the environment file variable to the required environment variables
            environment = required_environment.copy()
            environment[ENVIRONMENT_FILE_VAR] = container_environment_file.as_posix()

            # Step 2: write to the environment file
            local_environment_file.parent.mkdir(parents=True, exist_ok=True)
            write_env_file(writable_environment, local_environment_file)

            # Finally, add the environment file to the command
            pre_commands.append(f". ${{{ENVIRONMENT_FILE_VAR}}}".split(" "))

    # ------------------------------------------------------------------

    command_string = " && ".join([" ".join(_) for _ in pre_commands + [command]])
    logger.info(f"Final command string created: '{command_string}'")
    vol_configurations = [
        BatchEFSConfiguration(scratch_mount_point, read_only=False),
        BatchEFSConfiguration(shared_mount_point, read_only=True),
    ]
    if tmp_mount_point:
        vol_configurations.append(BatchEFSConfiguration(tmp_mount_point, read_only=False))
    logger.info(f"Constructing BatchJobBuilder instance...")
    return BatchJobBuilder(
        image=demand_execution.execution_image,
        job_definition_name=env_base.get_job_name(
            demand_execution.execution_type, demand_execution.get_execution_hash(False)
        ),
        job_name=env_base.get_job_name(
            demand_execution.execution_type, demand_execution.get_execution_hash(True)
        ),
        command=["/bin/bash", "-c", command_string],
        environment=environment,
        job_definition_tags={"USER": demand_execution.execution_metadata.user or "unknown"},
        resource_requirements=to_resource_requirements(
            gpu=demand_execution.resource_requirements.gpu,
            memory=demand_execution.resource_requirements.memory,
            vcpus=demand_execution.resource_requirements.vcpus,
        ),
        mount_points=[_.mount_point for _ in vol_configurations],
        volumes=[_.volume for _ in vol_configurations],
        env_base=env_base,
        # TODO: need to make this configurable
        privileged=True,
    )


def get_batch_job_queue_name(demand_execution: DemandExecution):
    aws_batch_exec_platform = demand_execution.execution_platform.aws_batch
    if aws_batch_exec_platform is None:
        raise ValueError("Demand execution does not have an AWS Batch execution platform")
    return aws_batch_exec_platform.job_queue_name
