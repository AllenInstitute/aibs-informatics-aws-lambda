from pathlib import Path
from typing import Any, Dict
from unittest import mock

from aibs_informatics_aws_utils.efs import MountPointConfiguration
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.demand_execution import DemandExecution
from aibs_informatics_core.models.unique_ids import UniqueID

from aibs_informatics_aws_lambda.common.handler import LambdaHandlerType
from aibs_informatics_aws_lambda.handlers.batch.model import CreateDefinitionAndPrepareArgsRequest
from aibs_informatics_aws_lambda.handlers.demand.context_manager import BatchEFSConfiguration
from aibs_informatics_aws_lambda.handlers.demand.model import (
    ContextManagerConfiguration,
    DemandExecutionCleanupConfigs,
    DemandExecutionSetupConfigs,
    DemandFileSystemConfigurations,
    EnvFileWriteMode,
    FileSystemConfiguration,
    PrepareBatchDataSyncRequest,
    PrepareDemandScaffoldingRequest,
    PrepareDemandScaffoldingResponse,
)
from aibs_informatics_aws_lambda.handlers.demand.scaffolding import (
    PrepareDemandScaffoldingHandler,
    construct_batch_efs_configuration,
)
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase
from test.aibs_informatics_aws_lambda.handlers.demand.test_context_manager import (
    get_any_demand_execution,
)


class PrepareDemandScaffoldingHandlerTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_MountPointConfiguration = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.demand.scaffolding.MountPointConfiguration"
        )
        self.mock_DemandExecutionContextManager = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.demand.scaffolding.DemandExecutionContextManager"
        )
        self.demand_execution = DemandExecution(
            execution_type="demand",
            execution_id=UniqueID.create(123),
            execution_image="image",
        )

    @property
    def handler(self) -> LambdaHandlerType:
        return PrepareDemandScaffoldingHandler.get_handler()

    def test__PrepareDemandScaffoldingRequest__deserialize(self) -> None:
        demand_execution = get_any_demand_execution()
        request_json = {
            "demand_execution": demand_execution.to_dict(),
            "file_system_configurations": {
                "scratch": {
                    "file_system": "fs-123456789012",
                    "access_point": "fsap-123456789012",
                    "container_path": "/opt/efs/scratch",
                },
            },
            "context_manager_configuration": {
                "isolate_inputs": True,
                "env_file_write_mode": "NEVER",
                "input_data_sync_configuration": {
                    "force": False,
                    "size_only": True,
                },
                "output_data_sync_configuration": {
                    "force": False,
                    "size_only": True,
                },
            },
        }

        actual = PrepareDemandScaffoldingRequest.from_dict(request_json)
        expected = PrepareDemandScaffoldingRequest(
            demand_execution=demand_execution,
            file_system_configurations=DemandFileSystemConfigurations(
                scratch=FileSystemConfiguration(
                    file_system="fs-123456789012",
                    access_point="fsap-123456789012",
                    container_path="/opt/efs/scratch",
                ),
            ),
            context_manager_configuration=ContextManagerConfiguration(
                isolate_inputs=True,
                env_file_write_mode=EnvFileWriteMode.NEVER,
            ),
        )
        assert actual == expected

    def test__PrepareDemandScaffoldingRequest__deserialize_only_required(self) -> None:
        demand_execution = get_any_demand_execution()
        request_json = {
            "demand_execution": demand_execution.to_dict(),
        }

        actual = PrepareDemandScaffoldingRequest.from_dict(request_json)
        expected = PrepareDemandScaffoldingRequest(
            demand_execution=demand_execution,
        )
        assert actual == expected

    def test__construct_batch_efs_configuration__works(self) -> None:
        # Arrange
        file_system_id = "fs-123456789012"
        access_point_id = "fsap-123456789012"
        container_path = "/opt/efs"
        read_only = False
        expected_mount_point_config = MountPointConfiguration(
            file_system={
                "FileSystemId": file_system_id,  # ID of the EFS file system
                "CreationToken": "string",  # Unique string to ensure idempotent creation
                "PerformanceMode": "generalPurpose",  # 'generalPurpose' or 'maxIO'
                "Encrypted": True,  # or False
                "KmsKeyId": "string",  # KMS key ID for encryption
                "ThroughputMode": "bursting",  # 'provisioned' or 'bursting'
                "ProvisionedThroughputInMibps": 123.0,  # if 'provisioned' is chosen
                "Tags": [
                    {"Key": "Name", "Value": "MyEFS"},
                ],
            },
            access_point={
                "AccessPointId": access_point_id,  # ID of the EFS access point
                "ClientToken": "string",  # Unique string to ensure idempotent creation
                "FileSystemId": file_system_id,  # ID of the EFS file system
                "PosixUser": {"Uid": "1000", "Gid": "1000"},
                "RootDirectory": {
                    "Path": "/",
                    "CreationInfo": {"OwnerUid": "1000", "OwnerGid": "1000", "Permissions": "755"},
                },
                "Tags": [
                    {"Key": "Name", "Value": "MyAccessPoint"},
                ],
            },
            mount_point=Path(container_path),
        )

        build = mock.MagicMock()
        self.mock_MountPointConfiguration.build = build
        build.return_value = expected_mount_point_config

        # Act
        handler = PrepareDemandScaffoldingHandler()
        result = construct_batch_efs_configuration(
            env_base=self.env_base,
            file_system=file_system_id,
            access_point=access_point_id,
            container_path=container_path,
            read_only=read_only,
        )

        # Assert
        self.assertEqual(
            result.mount_point,
            {
                "containerPath": container_path,
                "readOnly": False,
                "sourceVolume": "fs-123456789012-opt-efs-vol",
            },
        )
        self.assertEqual(result.read_only, read_only)

    @mock.patch(
        "aibs_informatics_aws_lambda.handlers.demand.scaffolding.construct_batch_efs_configuration"
    )
    def test__handle__simple_case(self, mock_construct_batch_efs_configuration) -> None:
        mock_construct_batch_efs_configuration.side_effect = (
            lambda *args, **kwargs: BatchEFSConfiguration(
                mount_point_config=MountPointConfiguration(
                    file_system=self.get_file_system("fs-123456789012"),
                    access_point=self.get_access_point("fsap-123456789012", "fs-123456789012"),
                    mount_point=Path("/opt/efs"),
                ),
                read_only=False,
            )
        )

        context_manager = mock.MagicMock()
        self.mock_DemandExecutionContextManager.return_value = context_manager
        context_manager.demand_execution = self.demand_execution
        context_manager.pre_execution_data_sync_requests = [
            PrepareBatchDataSyncRequest(
                source_path=S3Path("s3://bucket/src"),
                destination_path=S3Path("s3://bucket/dst"),
                temporary_request_payload_path=S3Path("s3://bucket/tmp"),
            )
        ]
        context_manager.post_execution_data_sync_requests = []
        batch_job_builder = mock.MagicMock()
        context_manager.batch_job_builder = batch_job_builder
        context_manager.batch_job_queue_name = "job_queue_name"
        batch_job_builder.image = "image"
        batch_job_builder.job_definition_name = "job_definition_name"
        batch_job_builder.job_name = "job_name"
        batch_job_builder.job_definition_tags = {}
        batch_job_builder.command = ["command"]
        batch_job_builder.environment = {"key": "value"}
        batch_job_builder.resource_requirements = []
        batch_job_builder.mount_points = []
        batch_job_builder.volumes = []
        batch_job_builder.privileged = False
        batch_job_builder.job_role_arn = None

        expected = PrepareDemandScaffoldingResponse(
            demand_execution=self.demand_execution,
            setup_configs=DemandExecutionSetupConfigs(
                data_sync_requests=[
                    PrepareBatchDataSyncRequest(
                        source_path=S3Path("s3://bucket/src"),
                        destination_path=S3Path("s3://bucket/dst"),
                        temporary_request_payload_path=S3Path("s3://bucket/tmp"),
                    )
                ],
                batch_create_request=CreateDefinitionAndPrepareArgsRequest(
                    image="image",
                    job_definition_name="job_definition_name",
                    job_name="job_name",
                    job_queue_name="job_queue_name",
                    job_definition_tags={},
                    command=["command"],
                    environment={"key": "value"},
                    resource_requirements=[],
                    mount_points=[],
                    volumes=[],
                    retry_strategy={
                        "attempts": 5,
                        "evaluateOnExit": [
                            {
                                "action": "RETRY",
                                "onReason": "DockerTimeoutError*",
                                "onStatusReason": "Task failed to start",
                            },
                            {"action": "RETRY", "onStatusReason": "Host EC2*"},
                            {"action": "EXIT", "onStatusReason": "*"},
                        ],
                    },
                ),
            ),
            cleanup_configs=DemandExecutionCleanupConfigs(data_sync_requests=[]),
        ).to_dict()

        self.assertHandles(
            self.handler,
            {
                "demand_execution": self.demand_execution.to_dict(),
            },
            response=expected,
        )
        assert mock_construct_batch_efs_configuration.call_args_list == [
            mock.call(
                env_base=self.env_base,
                file_system=None,
                access_point="scratch",
                container_path="/opt/efs/scratch",
                read_only=False,
            ),
            mock.call(
                env_base=self.env_base,
                file_system=None,
                access_point="shared",
                container_path="/opt/efs/shared",
                read_only=True,
            ),
        ]

    @mock.patch(
        "aibs_informatics_aws_lambda.handlers.demand.scaffolding.construct_batch_efs_configuration"
    )
    def test__handle__file_system_overrides(self, mock_construct_batch_efs_configuration) -> None:
        mock_construct_batch_efs_configuration.side_effect = (
            lambda *args, **kwargs: BatchEFSConfiguration(
                mount_point_config=MountPointConfiguration(
                    file_system=self.get_file_system("fs-123456789012"),
                    access_point=self.get_access_point("fsap-123456789012", "fs-123456789012"),
                    mount_point=Path("/opt/efs"),
                ),
                read_only=False,
            )
        )

        context_manager = mock.MagicMock()
        self.mock_DemandExecutionContextManager.return_value = context_manager
        context_manager.demand_execution = self.demand_execution
        context_manager.pre_execution_data_sync_requests = []
        context_manager.post_execution_data_sync_requests = []
        batch_job_builder = mock.MagicMock()
        context_manager.batch_job_builder = batch_job_builder
        context_manager.batch_job_queue_name = "job_queue_name"
        batch_job_builder.image = "image"
        batch_job_builder.job_definition_name = "job_definition_name"
        batch_job_builder.job_name = "job_name"
        batch_job_builder.job_definition_tags = {}
        batch_job_builder.command = ["command"]
        batch_job_builder.environment = {"key": "value"}
        batch_job_builder.resource_requirements = []
        batch_job_builder.mount_points = []
        batch_job_builder.volumes = []
        batch_job_builder.privileged = False
        batch_job_builder.job_role_arn = None

        expected = PrepareDemandScaffoldingResponse(
            demand_execution=self.demand_execution,
            setup_configs=DemandExecutionSetupConfigs(
                data_sync_requests=[],
                batch_create_request=CreateDefinitionAndPrepareArgsRequest(
                    image="image",
                    job_definition_name="job_definition_name",
                    job_name="job_name",
                    job_queue_name="job_queue_name",
                    job_definition_tags={},
                    command=["command"],
                    environment={"key": "value"},
                    resource_requirements=[],
                    mount_points=[],
                    volumes=[],
                    retry_strategy={
                        "attempts": 5,
                        "evaluateOnExit": [
                            {
                                "action": "RETRY",
                                "onReason": "DockerTimeoutError*",
                                "onStatusReason": "Task failed to start",
                            },
                            {"action": "RETRY", "onStatusReason": "Host EC2*"},
                            {"action": "EXIT", "onStatusReason": "*"},
                        ],
                    },
                ),
            ),
            cleanup_configs=DemandExecutionCleanupConfigs(data_sync_requests=[]),
        ).to_dict()

        self.assertHandles(
            self.handler,
            {
                "demand_execution": self.demand_execution.to_dict(),
                "file_system_configurations": {
                    "scratch": {
                        "file_system": "fs-123456789012",
                        "access_point": "fsap-123456789012",
                        "container_path": "/opt/efs/anotherscratch",
                    },
                    "shared": {
                        "file_system": "fs-123456789012",
                        "access_point": "fsap-1234567890123",
                        "container_path": "/opt/efs/anothershared",
                    },
                },
            },
            response=expected,
        )
        assert mock_construct_batch_efs_configuration.call_args_list == [
            mock.call(
                env_base=self.env_base,
                file_system="fs-123456789012",
                access_point="fsap-123456789012",
                container_path="/opt/efs/anotherscratch",
                read_only=False,
            ),
            mock.call(
                env_base=self.env_base,
                file_system="fs-123456789012",
                access_point="fsap-1234567890123",
                container_path="/opt/efs/anothershared",
                read_only=True,
            ),
        ]

    def get_file_system(self, file_system_id: str) -> Dict[str, Any]:
        return {
            "FileSystemId": file_system_id,  # ID of the EFS file system
            "CreationToken": "string",  # Unique string to ensure idempotent creation
            "PerformanceMode": "generalPurpose",  # 'generalPurpose' or 'maxIO'
            "Encrypted": True,  # or False
            "KmsKeyId": "string",  # KMS key ID for encryption
            "ThroughputMode": "bursting",  # 'provisioned' or 'bursting'
            "ProvisionedThroughputInMibps": 123.0,  # if 'provisioned' is chosen
            "Tags": [
                {"Key": "Name", "Value": "MyEFS"},
            ],
        }

    def get_access_point(
        self, access_point_id: str, file_system_id: str, **tags
    ) -> Dict[str, Any]:
        return {
            "AccessPointId": access_point_id,  # ID of the EFS access point
            "ClientToken": "string",  # Unique string to ensure idempotent creation
            "FileSystemId": file_system_id,  # ID of the EFS file system
            "PosixUser": {"Uid": "1000", "Gid": "1000"},
            "RootDirectory": {
                "Path": "/",
                "CreationInfo": {"OwnerUid": "1000", "OwnerGid": "1000", "Permissions": "755"},
            },
            "Tags": [
                {"Key": "Name", "Value": "value"},
            ],
        }
