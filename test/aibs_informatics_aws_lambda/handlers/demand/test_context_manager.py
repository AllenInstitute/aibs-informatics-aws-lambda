from datetime import datetime
from pathlib import Path
from test.base import AwsBaseTest, does_not_raise
from typing import Dict, Optional, Union

import boto3
from aibs_informatics_aws_utils.batch import BatchJobBuilder
from aibs_informatics_aws_utils.constants.efs import (
    EFS_ROOT_ACCESS_POINT_NAME,
    EFS_ROOT_PATH,
    EFS_SCRATCH_ACCESS_POINT_NAME,
    EFS_SCRATCH_PATH,
    EFS_SHARED_ACCESS_POINT_NAME,
    EFS_SHARED_PATH,
    EFS_TMP_ACCESS_POINT_NAME,
    EFS_TMP_PATH,
)
from aibs_informatics_aws_utils.efs import MountPointConfiguration
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.models.demand_execution import (
    DemandExecution,
    DemandExecutionMetadata,
    DemandExecutionParameters,
    DemandResourceRequirements,
)
from aibs_informatics_core.models.demand_execution.platform import (
    AWSBatchExecutionPlatform,
    ExecutionPlatform,
)
from aibs_informatics_core.models.unique_ids import UniqueID
from aibs_informatics_core.utils.hashing import uuid_str
from moto import mock_efs, mock_sts
from pytest import fixture, mark, param

from aibs_informatics_aws_lambda.handlers.demand.context_manager import (
    BatchEFSConfiguration,
    DemandExecutionContextManager,
    get_batch_efs_configuration,
    get_batch_job_queue_name,
    update_demand_execution_parameter_inputs,
)
from aibs_informatics_aws_lambda.handlers.demand.model import (
    ContextManagerConfiguration,
    EnvFileWriteMode,
)

ENV_BASE = EnvBase("dev-marmotdev")
DEMAND_ID = UniqueID.create()
ANOTHER_DEMAND_ID = UniqueID.create()

S3_URI = S3URI.build(bucket_name="bucket", key="key")
ANOTHER_S3_URI = S3URI.build(bucket_name="bucket", key="another_key")


EXECUTION_IMAGE = "051791135335.dkr.ecr.us-west-2.amazonaws.com/test_image:latest"
ANOTHER_EXECUTION_IMAGE = "051791135335.dkr.ecr.us-west-2.amazonaws.com/another_image:latest"


EFS_SCRATCH_MOUNT_PATH = Path("/mnt/efs")


def get_any_demand_execution(
    execution_id: Optional[str] = None,
    execution_type: Optional[str] = None,
    execution_image: Optional[str] = None,
    execution_parameters: Optional[DemandExecutionParameters] = None,
    execution_metadata: Optional[DemandExecutionMetadata] = None,
    execution_resource_requirements: Optional[DemandResourceRequirements] = None,
    execution_platform: Optional[ExecutionPlatform] = None,
) -> DemandExecution:
    return DemandExecution(
        execution_id=execution_id or DEMAND_ID,
        execution_type=execution_type or "custom",
        execution_image=execution_image or EXECUTION_IMAGE,
        execution_parameters=execution_parameters
        or DemandExecutionParameters(
            output_s3_prefix=S3_URI,
            verbosity=True,
        ),
        resource_requirements=execution_resource_requirements
        or DemandResourceRequirements(memory=8192, vcpus=4),
        execution_metadata=execution_metadata or DemandExecutionMetadata(),
        execution_platform=execution_platform
        or ExecutionPlatform(aws_batch=AWSBatchExecutionPlatform(job_queue_name="queue")),
    )


@fixture(scope="function", name="efs")
def efs_client_fixture(aws_credentials_fixture):
    with mock_efs():
        yield boto3.client("efs")


@fixture(scope="function", name="get_or_create_file_system")
def get_or_create_file_system_fixture(efs):
    _cache = {}

    def get_or_create_file_system(
        file_system_name: Optional[str] = None, tags: Optional[Dict[str, str]] = None
    ) -> str:
        file_system_name = file_system_name or "test_file_system"
        if file_system_name not in _cache:
            tags_list = [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
            tags_list.insert(0, {"Key": "Name", "Value": file_system_name})
            _cache[file_system_name] = efs.create_file_system(
                CreationToken=file_system_name,
                Tags=tags_list,
            )
        return _cache[file_system_name]["FileSystemId"]

    return get_or_create_file_system


@fixture(scope="function", name="create_access_point")
def create_access_point_fixture(efs):
    def create_access_point(
        file_system_id: str,
        access_point_name: str,
        path: str,
        tags: Optional[Dict[str, str]] = None,
    ):
        tags_list = [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
        tags_list.insert(0, {"Key": "Name", "Value": access_point_name})
        response = efs.create_access_point(
            FileSystemId=file_system_id,
            PosixUser={"Uid": 1000, "Gid": 1000},
            RootDirectory={"Path": path},
            Tags=tags_list,
        )
        return response["AccessPointArn"].split("/")[-1]

    return create_access_point


def test__update_demand_execution_parameter_inputs__works(
    get_or_create_file_system, create_access_point
):
    fs_id = get_or_create_file_system("fs")
    ap_id = create_access_point(fs_id, "ap", "/opt/efs")

    efs_mount_point_config = MountPointConfiguration.build(
        "/mnt/efs", file_system=fs_id, access_point=ap_id
    )

    demand_execution = get_any_demand_execution(
        execution_parameters=DemandExecutionParameters(
            command=["cmd"],
            inputs=["X"],
            params={"X": S3_URI / "in"},
            output_s3_prefix=S3_URI,
        )
    )

    demand_execution = update_demand_execution_parameter_inputs(
        demand_execution=demand_execution,
        container_shared_path=efs_mount_point_config.mount_point,
        container_working_path=efs_mount_point_config.mount_point,
    )

    job_inputs = demand_execution.execution_parameters.job_param_inputs

    assert len(job_inputs) == 1
    assert job_inputs[0].value.startswith(efs_mount_point_config.mount_point.as_posix())


def test__update_demand_execution_parameter_inputs__isolates_inputs(
    get_or_create_file_system, create_access_point
):
    fs_id = get_or_create_file_system("fs")
    ap_id = create_access_point(fs_id, "ap", "/opt/efs")

    efs_mount_point_config = MountPointConfiguration.build(
        "/mnt/efs", file_system=fs_id, access_point=ap_id
    )

    demand_execution = get_any_demand_execution(
        execution_parameters=DemandExecutionParameters(
            command=["cmd"],
            inputs=["X"],
            params={
                "X": {
                    "local": "X",
                    "remote": S3_URI / "in",
                }
            },
            output_s3_prefix=S3_URI,
        )
    )

    demand_execution = update_demand_execution_parameter_inputs(
        demand_execution,
        container_shared_path=efs_mount_point_config.mount_point,
        container_working_path=Path(f"/opt/tmp/{demand_execution.execution_id}"),
        isolate_inputs=True,
    )

    job_inputs = demand_execution.execution_parameters.job_param_inputs

    assert len(job_inputs) == 1
    assert job_inputs[0].value.startswith("/opt/tmp")
    assert job_inputs[0].value.endswith(f"/{DEMAND_ID}/X")


def test__BatchEFSConfiguration__build__works(get_or_create_file_system, create_access_point):
    fs_id = get_or_create_file_system("fs")
    ap_id = create_access_point(fs_id, "access_point", "/opt/efs")

    batch_efs_configuration = BatchEFSConfiguration.build(
        access_point=ap_id, mount_path="/mnt/efs"
    )

    expected_volume_name = f"fs-mnt-efs-vol"

    expected_volume = {
        "name": expected_volume_name,
        "efsVolumeConfiguration": {
            "fileSystemId": fs_id,
            "rootDirectory": "/",
            "transitEncryption": "ENABLED",
            "authorizationConfig": {
                "accessPointId": ap_id,
                "iam": "DISABLED",
            },
        },
    }
    expected_mount_point = {
        "containerPath": "/mnt/efs",
        "readOnly": False,
        "sourceVolume": expected_volume_name,
    }

    assert batch_efs_configuration.mount_path == Path("/mnt/efs")
    assert batch_efs_configuration.volume == expected_volume
    assert batch_efs_configuration.mount_point == expected_mount_point


def test__get_batch_efs_configuration__defaults_work(
    get_or_create_file_system, create_access_point
):
    env_base = EnvBase("dev-marmotdev")

    fs_id = get_or_create_file_system(
        env_base.get_resource_name("fs"),
        tags={env_base.ENV_BASE_KEY: env_base},
    )
    create_access_point(
        fs_id,
        EFS_SCRATCH_ACCESS_POINT_NAME,
        EFS_SCRATCH_MOUNT_PATH.as_posix(),
        tags={env_base.ENV_BASE_KEY: env_base},
    )
    config = get_batch_efs_configuration(
        env_base=env_base,
        container_path=EFS_SCRATCH_MOUNT_PATH.as_posix(),
        file_system_name="fs",
        access_point_name=EFS_SCRATCH_ACCESS_POINT_NAME,
        read_only=False,
    )
    assert config.mount_path == EFS_SCRATCH_MOUNT_PATH


def test__generate_batch_job_builder__simple(get_or_create_file_system, create_access_point):
    demand_execution = get_any_demand_execution(
        execution_parameters=DemandExecutionParameters(
            command=["blah"],
            inputs=["A", "B"],
            outputs=["C", "D"],
            params={
                "A": S3_URI,
                "B": ANOTHER_S3_URI,
                "C": "c",
                "D": f"d @ {ANOTHER_S3_URI}-out",
            },
            output_s3_prefix=S3URI.build("bucket", key="outs/"),
        ),
    )


class Helpers:
    def get_any_file_system_details(self, **kwargs):
        return {
            "OwnerId": "owner",
            "CreationToken": "token",
            "FileSystemId": "fs-abcdef12345",
            "FileSystemName": kwargs.get("name", kwargs.get("FileSystemName", "name")),
            "CreationTime": datetime(2023, 1, 1, 0, 0, 0, 0),
            "LifeCycleState": "available",
            "NumberOfMountTargets": 1,
            "SizeInBytes": {"Value": 123},
            "PerformanceMode": "generalPurpose",
            "Tags": [],
        }

    def get_any_access_point_details(self, **kwargs):
        access_point_tags = kwargs.get("access_point_tags", {"Name": "scratch"})
        return {
            **{
                "ClientToken": "token",
                "AccessPointId": "fs-ap12345678",
                "OwnerId": "owner",
                "RootDirectory": {"Path": f"/{access_point_tags.get('Name')}"},
                "Tags": [],
            },
            **kwargs,
        }


@mock_sts
class DemandExecutionContextManagerTests(AwsBaseTest, Helpers):
    def setUp(self) -> None:
        super().setUp()
        self._file_store_name_id_map = {}
        self.mock_efs = mock_efs()
        self.mock_efs.start()
        self.set_aws_credentials()
        self.setUpEFS()

    def tearDown(self) -> None:
        self.mock_efs.stop()
        return super().tearDown()

    def setUpEFS(self):
        self.gwo_file_system_id = self.create_file_system(
            self.env_base.get_resource_name("fs"),
            tags={self.env_base.ENV_BASE_KEY: self.env_base},
        )
        self.access_point_id__root = self.create_access_point(
            EFS_ROOT_ACCESS_POINT_NAME,
            f"{EFS_ROOT_PATH}",
            self.gwo_file_system_id,
            tags={self.env_base.ENV_BASE_KEY: self.env_base},
        )
        self.access_point_id__shared = self.create_access_point(
            EFS_SHARED_ACCESS_POINT_NAME,
            f"{EFS_SHARED_PATH}",
            self.gwo_file_system_id,
            tags={self.env_base.ENV_BASE_KEY: self.env_base},
        )
        self.access_point_id__scratch = self.create_access_point(
            EFS_SCRATCH_ACCESS_POINT_NAME,
            f"{EFS_SCRATCH_PATH}",
            self.gwo_file_system_id,
            tags={self.env_base.ENV_BASE_KEY: self.env_base},
        )
        # HACK: on macos, /tmp is a symlink to /private/tmp. This causes problems in the
        #       resolution and mapping of mounted paths to efs paths because we use the
        #       `pathlib,Path.resolve` to get the real path. This method will resolve the
        #       symlink to the real path. This is not what we want. We want to keep the
        #       symlink in the path. But there is no other method in `pathlib.Path` that
        #       will normalize the path.
        #       Solution here is to make tmp -> tmpdir. This is a hack and should not be
        #       a problem in linux based machines in production.
        self.access_point_id__tmp = self.create_access_point(
            EFS_TMP_ACCESS_POINT_NAME,
            f"{EFS_TMP_PATH}dir",
            self.gwo_file_system_id,
            tags={self.env_base.ENV_BASE_KEY: self.env_base},
        )

        self.root_mount_point = self.tmp_path()
        self.set_env_vars(
            *MountPointConfiguration.to_env_vars(
                self.root_mount_point, self.access_point_id__root
            ).items(),
        )
        (self.root_mount_point / "shared").mkdir()
        (self.root_mount_point / "scratch").mkdir()
        (self.root_mount_point / "tmp").mkdir()

    @property
    def efs_client(self):
        return boto3.client("efs")

    def create_file_system(
        self, file_system_name: Optional[str] = None, tags: Optional[Dict[str, str]] = None
    ):
        file_system_name = file_system_name or "fs"
        if file_system_name not in self._file_store_name_id_map:
            tags_list = [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
            tags_list.insert(0, {"Key": "Name", "Value": file_system_name})
            fs_response = self.efs_client.create_file_system(
                CreationToken=file_system_name,
                Tags=tags_list,
            )
            self._file_store_name_id_map[file_system_name] = fs_response["FileSystemId"]
        return self._file_store_name_id_map[file_system_name]

    def create_access_point(
        self,
        access_point_name: str,
        access_point_path: Union[Path, str] = Path("/"),
        file_system_id: Optional[str] = None,
        file_system_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        file_system_id = file_system_id or self.create_file_system(file_system_name)
        access_point_path = Path(access_point_path)

        tags_list = [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
        tags_list.insert(0, {"Key": "Name", "Value": access_point_name})
        response = self.efs_client.create_access_point(
            FileSystemId=file_system_id,
            PosixUser={"Uid": 1000, "Gid": 1000},
            RootDirectory={"Path": str(access_point_path)},
            Tags=tags_list,
        )
        return response["AccessPointId"]

    def test__init__with_tmp_vol_configuration(self):
        demand_execution = get_any_demand_execution(
            execution_id=(demand_id := uuid_str("123")),
            execution_parameters=DemandExecutionParameters(
                command=["cmd"],
            ),
        )

        vol_configuration = get_batch_efs_configuration(
            env_base=self.env_base,
            container_path=f"/opt/efs{EFS_SCRATCH_PATH}",
            access_point_name=EFS_SCRATCH_ACCESS_POINT_NAME,
            read_only=False,
        )
        shared_vol_configuration = get_batch_efs_configuration(
            env_base=self.env_base,
            container_path=f"/opt/efs{EFS_SHARED_PATH}",
            access_point_name=EFS_SHARED_ACCESS_POINT_NAME,
            read_only=True,
        )
        tmp_vol_configuration = get_batch_efs_configuration(
            env_base=self.env_base,
            container_path=f"/opt/efs{EFS_TMP_PATH}dir",
            access_point_name=EFS_TMP_ACCESS_POINT_NAME,
            read_only=False,
        )

        decm = DemandExecutionContextManager(
            demand_execution=demand_execution,
            scratch_vol_configuration=vol_configuration,
            shared_vol_configuration=shared_vol_configuration,
            tmp_vol_configuration=tmp_vol_configuration,
            configuration=ContextManagerConfiguration(),
            env_base=self.env_base,
        )

        self.assertEqual(len(decm.efs_mount_points), 3)
        self.assertEqual(decm.container_shared_path, Path("/opt/efs/shared"))
        self.assertEqual(decm.container_working_path, Path(f"/opt/efs/scratch/{demand_id}"))
        self.assertEqual(decm.container_tmp_path, Path("/opt/efs/tmpdir"))

        bjb = decm.batch_job_builder
        self.assertEqual(len(bjb.mount_points), 3)

    def test__container_and_efs_path_properties__are_as_expected(self):
        demand_execution = get_any_demand_execution(
            execution_id=(demand_id := uuid_str("123")),
            execution_parameters=DemandExecutionParameters(
                command=["cmd"],
            ),
        )
        decm = DemandExecutionContextManager.from_demand_execution(demand_execution, self.env_base)
        self.assertEqual(decm.container_shared_path, Path("/opt/efs/shared"))
        self.assertEqual(decm.container_working_path, Path(f"/opt/efs/scratch/{demand_id}"))
        self.assertEqual(decm.container_tmp_path, Path("/opt/efs/scratch/tmp"))

        self.assertEqual(decm.efs_shared_path, EFSPath(f"{self.gwo_file_system_id}:/shared"))
        self.assertEqual(
            decm.efs_working_path, EFSPath(f"{self.gwo_file_system_id}:/scratch/{demand_id}")
        )
        self.assertEqual(decm.efs_tmp_path, EFSPath(f"{self.gwo_file_system_id}:/scratch/tmp"))

    def test__batch_job_builder__always_write_mode(self):
        demand_execution = get_any_demand_execution(
            execution_id=uuid_str("123"),
            execution_parameters=DemandExecutionParameters(
                command=["cmd", "${ENV_BASE}", "${A}/${B}"],
                params={"A": "a", "B": "b", "C": "c"},
            ),
        )
        decm = DemandExecutionContextManager.from_demand_execution(
            demand_execution,
            self.env_base,
            configuration=ContextManagerConfiguration(env_file_write_mode=EnvFileWriteMode.ALWAYS),
        )
        actual = decm.batch_job_builder
        self.assertEqual(actual.image, demand_execution.execution_image)
        self.assertStringPattern("dev-marmotdev-custom-[a-f0-9]{64}", actual.job_definition_name)

        # Assert environment and environment file are as expected
        env_file = (
            self.root_mount_point / "scratch" / demand_execution.execution_id / ".demand.env"
        )
        assert env_file.exists()
        assert env_file.read_text() == (
            f'export EXECUTION_ID="{demand_execution.execution_id}"\n' 'export C="c"'
        )
        assert actual.environment == {
            "ENV_BASE": "dev-marmotdev",
            "AWS_REGION": "us-west-2",
            "_ENVIRONMENT_FILE": f"/opt/efs/scratch/{demand_execution.execution_id}/.demand.env",
            "WORKING_DIR": f"/opt/efs/scratch/{demand_execution.execution_id}",
            "TMPDIR": "/opt/efs/scratch/tmp",
            "A": "a",
            "B": "b",
        }
        assert actual.command == [
            "/bin/bash",
            "-c",
            "mkdir -p ${WORKING_DIR} && mkdir -p ${TMPDIR} && cd ${WORKING_DIR} && . ${_ENVIRONMENT_FILE} && cmd ${ENV_BASE} ${A}/${B}",
        ]

    def test__batch_job_builder__never_write_mode(self):
        demand_execution = get_any_demand_execution(
            execution_id=uuid_str("123"),
            execution_parameters=DemandExecutionParameters(
                command=["cmd", "${ENV_BASE}", "${A}/${B}"],
                params={"A": "a", "B": "b", "C": "c"},
            ),
        )
        decm = DemandExecutionContextManager.from_demand_execution(
            demand_execution,
            self.env_base,
            configuration=ContextManagerConfiguration(env_file_write_mode=EnvFileWriteMode.NEVER),
        )
        actual = decm.batch_job_builder
        self.assertEqual(actual.image, demand_execution.execution_image)
        self.assertStringPattern("dev-marmotdev-custom-[a-f0-9]{64}", actual.job_definition_name)

        # Assert environment and environment file are as expected
        env_file = (
            self.root_mount_point / "scratch" / demand_execution.execution_id / ".demand.env"
        )
        assert not env_file.exists()
        assert actual.environment == {
            "ENV_BASE": "dev-marmotdev",
            "AWS_REGION": "us-west-2",
            "WORKING_DIR": f"/opt/efs/scratch/{demand_execution.execution_id}",
            "TMPDIR": "/opt/efs/scratch/tmp",
            "EXECUTION_ID": demand_execution.execution_id,
            "A": "a",
            "B": "b",
            "C": "c",
        }
        assert actual.command == [
            "/bin/bash",
            "-c",
            "mkdir -p ${WORKING_DIR} && mkdir -p ${TMPDIR} && cd ${WORKING_DIR} && cmd ${ENV_BASE} ${A}/${B}",
        ]

    def test__batch_job_builder__conditional_write_mode__not_required(self):
        demand_execution = get_any_demand_execution(
            execution_id=uuid_str("123"),
            execution_parameters=DemandExecutionParameters(
                command=["cmd", "${ENV_BASE}", "${A}/${B}"],
                params={"A": "a", "B": "b", "C": "c"},
            ),
        )
        decm = DemandExecutionContextManager.from_demand_execution(
            demand_execution,
            self.env_base,
            configuration=ContextManagerConfiguration(
                env_file_write_mode=EnvFileWriteMode.IF_REQUIRED
            ),
        )
        actual = decm.batch_job_builder
        self.assertEqual(actual.image, demand_execution.execution_image)
        self.assertStringPattern("dev-marmotdev-custom-[a-f0-9]{64}", actual.job_definition_name)

        # Assert environment and environment file are as expected
        env_file = (
            self.root_mount_point / "scratch" / demand_execution.execution_id / ".demand.env"
        )
        assert not env_file.exists()
        assert actual.environment == {
            "ENV_BASE": "dev-marmotdev",
            "AWS_REGION": "us-west-2",
            "WORKING_DIR": f"/opt/efs/scratch/{demand_execution.execution_id}",
            "TMPDIR": "/opt/efs/scratch/tmp",
            "EXECUTION_ID": demand_execution.execution_id,
            "A": "a",
            "B": "b",
            "C": "c",
        }
        assert actual.command == [
            "/bin/bash",
            "-c",
            "mkdir -p ${WORKING_DIR} && mkdir -p ${TMPDIR} && cd ${WORKING_DIR} && cmd ${ENV_BASE} ${A}/${B}",
        ]

    def test__batch_job_builder__conditional_write_mode__required(self):
        demand_execution = get_any_demand_execution(
            execution_id=uuid_str("123"),
            execution_parameters=DemandExecutionParameters(
                command=["cmd", "${ENV_BASE}"],
                params={
                    # This should be greater than 8192
                    f"VAR_{i}": f"VAL_{i}"
                    for i in range(1000, 2000)
                },
            ),
        )
        decm = DemandExecutionContextManager.from_demand_execution(
            demand_execution,
            self.env_base,
            configuration=ContextManagerConfiguration(
                env_file_write_mode=EnvFileWriteMode.IF_REQUIRED
            ),
        )
        actual = decm.batch_job_builder
        self.assertEqual(actual.image, demand_execution.execution_image)
        self.assertStringPattern("dev-marmotdev-custom-[a-f0-9]{64}", actual.job_definition_name)

        # Assert environment and environment file are as expected
        env_file = (
            self.root_mount_point / "scratch" / demand_execution.execution_id / ".demand.env"
        )
        assert env_file.exists()
        assert env_file.read_text() == (
            f'export EXECUTION_ID="{demand_execution.execution_id}"\n'
            + "\n".join([f'export VAR_{i}="VAL_{i}"' for i in range(1000, 2000)])
        )
        assert actual.environment == {
            "ENV_BASE": "dev-marmotdev",
            "AWS_REGION": "us-west-2",
            "_ENVIRONMENT_FILE": f"/opt/efs/scratch/{demand_execution.execution_id}/.demand.env",
            "WORKING_DIR": f"/opt/efs/scratch/{demand_execution.execution_id}",
            "TMPDIR": "/opt/efs/scratch/tmp",
        }
        assert actual.command == [
            "/bin/bash",
            "-c",
            "mkdir -p ${WORKING_DIR} && mkdir -p ${TMPDIR} && cd ${WORKING_DIR} && . ${_ENVIRONMENT_FILE} && cmd ${ENV_BASE}",
        ]

    def test__pre_execution_data_sync_requests__no_inputs_generate_empty_list(self):
        demand_execution = get_any_demand_execution(
            execution_parameters=DemandExecutionParameters(
                command=["cmd"],
            )
        )
        decm = DemandExecutionContextManager.from_demand_execution(demand_execution, self.env_base)
        self.assertTrue(len(decm.pre_execution_data_sync_requests) == 0)

    def test__pre_execution_data_sync_requests__single_input_generates_list(self):
        demand_execution = get_any_demand_execution(
            execution_parameters=DemandExecutionParameters(
                command=["cmd"], inputs=["X"], params={"X": S3_URI}
            )
        )
        decm = DemandExecutionContextManager.from_demand_execution(demand_execution, self.env_base)
        actual = decm.pre_execution_data_sync_requests

        expected = {
            "retain_source_data": True,
            "source_path": S3_URI,
            "destination_path": f"{self.gwo_file_system_id}:/shared/558ca1533e03aaea2e3fb825be29124c1648046a2893052d1a1df0059becbf4f",
        }
        self.assertTrue(len(actual) == 1)
        actual_dict = actual[0].to_dict()

        self.assertEqual(expected["source_path"], actual_dict.get("source_path"))
        self.assertEqual(expected["destination_path"], actual_dict.get("destination_path"))
        self.assertEqual(expected["retain_source_data"], actual_dict.get("retain_source_data"))

    def test__post_execution_data_sync_requests__no_outputs_generate_empty_list(self):
        demand_execution = get_any_demand_execution(
            execution_parameters=DemandExecutionParameters(
                command=["cmd"], inputs=["X"], params={"X": S3_URI}
            )
        )
        decm = DemandExecutionContextManager.from_demand_execution(demand_execution, self.env_base)
        self.assertTrue(len(decm.post_execution_data_sync_requests) == 0)

    def test__post_execution_data_sync_requests__single_output_generates_list(self):
        demand_execution = get_any_demand_execution(
            execution_parameters=DemandExecutionParameters(
                command=["cmdx"], outputs=["X"], params={"X": "outs"}, output_s3_prefix=S3_URI
            )
        )
        decm = DemandExecutionContextManager.from_demand_execution(demand_execution, self.env_base)
        actual = decm.post_execution_data_sync_requests

        expected = {
            "retain_source_data": False,
            "source_path": f"{self.gwo_file_system_id}:/scratch/{demand_execution.execution_id}/outs",
            "destination_path": f"{S3_URI}/outs",
        }
        self.assertTrue(len(actual) == 1)
        actual_dict = actual[0].to_dict()
        self.assertEqual(expected["source_path"], actual_dict.get("source_path"))
        self.assertEqual(expected["destination_path"], actual_dict.get("destination_path"))
        self.assertEqual(expected["retain_source_data"], actual_dict.get("retain_source_data"))

    def test__batch_job_queue_name__works_for_valid_demand_execution(self):
        demand_execution = get_any_demand_execution(
            execution_parameters=DemandExecutionParameters(
                command=["cmd"],
            ),
            execution_platform=ExecutionPlatform(
                aws_batch=AWSBatchExecutionPlatform(job_queue_name="queue")
            ),
        )
        context_manager = DemandExecutionContextManager.from_demand_execution(
            demand_execution, self.env_base
        )

        assert context_manager.batch_job_queue_name == "queue"

    def test__batch_job_queue_name__raises_error_for_invalid_demand_execution(self):
        demand_execution = get_any_demand_execution(
            execution_parameters=DemandExecutionParameters(command=["cmd"]),
            execution_platform=ExecutionPlatform(),
        )
        context_manager = DemandExecutionContextManager.from_demand_execution(
            demand_execution, self.env_base
        )

        with self.assertRaises(ValueError):
            context_manager.batch_job_queue_name
