from test.base import does_not_raise

from aibs_informatics_core.exceptions import ValidationError
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.email_address import EmailAddress
from pytest import mark, param, raises

from aibs_informatics_aws_lambda.handlers.data_sync.model import RemoveDataPathsRequest
from aibs_informatics_aws_lambda.handlers.demand.model import (
    DataSyncRequest,
    DemandExecutionCleanupConfigs,
    DemandExecutionSetupConfigs,
    DemandFileSystemConfigurations,
    FileSystemConfiguration,
    FileSystemSelectionStrategy,
    PrepareBatchDataSyncRequest,
)


@mark.parametrize(
    "input_value, expected, raise_expectation",
    [
        param(
            DemandExecutionCleanupConfigs(
                data_sync_requests=[
                    PrepareBatchDataSyncRequest(
                        source_path=S3Path("s3://bucket/src"),
                        destination_path=S3Path("s3://bucket/dst"),
                        temporary_request_payload_path=S3Path("s3://bucket/tmp"),
                    )
                ],
            ),
            {
                "data_sync_requests": [
                    {
                        "destination_path": "s3://bucket/dst",
                        "fail_if_missing": True,
                        "force": False,
                        "include_detailed_response": False,
                        "max_concurrency": 25,
                        "remote_to_local_config": {"use_custom_tmp_dir": False},
                        "require_lock": False,
                        "retain_source_data": True,
                        "size_only": False,
                        "source_path": "s3://bucket/src",
                        "temporary_request_payload_path": "s3://bucket/tmp",
                    }
                ],
                "remove_data_paths_requests": [],
            },
            does_not_raise(),
            id="Handles single PrepareBatchDataSyncRequest",
        ),
        param(
            DemandExecutionCleanupConfigs(
                data_sync_requests=[
                    PrepareBatchDataSyncRequest(
                        source_path=S3Path("s3://bucket/src"),
                        destination_path=S3Path("s3://bucket/dst"),
                    )
                ]
            ),
            {
                "data_sync_requests": [
                    {
                        "destination_path": "s3://bucket/dst",
                        "fail_if_missing": True,
                        "force": False,
                        "include_detailed_response": False,
                        "max_concurrency": 25,
                        "remote_to_local_config": {"use_custom_tmp_dir": False},
                        "require_lock": False,
                        "retain_source_data": True,
                        "size_only": False,
                        "source_path": "s3://bucket/src",
                    }
                ],
                "remove_data_paths_requests": [],
            },
            does_not_raise(),
            id="Handles single ambiguous ds request",
        ),
        param(
            DemandExecutionCleanupConfigs(
                data_sync_requests=[],
                remove_data_paths_requests=[
                    RemoveDataPathsRequest(paths=["efs://path1", "efs://path2"])
                ],
            ),
            {
                "data_sync_requests": [],
                "remove_data_paths_requests": [{"paths": ["efs://path1", "efs://path2"]}],
            },
            does_not_raise(),
            id="Handles remove data path request, empty data sync requests",
        ),
    ],
)
def test__DemandExecutionCleanupConfigs__serialization(
    input_value: DemandExecutionCleanupConfigs, expected, raise_expectation
):
    with raise_expectation:
        actual = input_value.to_dict()
    if expected:
        assert expected == actual


@mark.parametrize(
    "input_value, expected, raise_expectation",
    [
        param(
            {
                "data_sync_requests": [
                    {
                        "destination_path": "s3://bucket/dst",
                        "fail_if_missing": True,
                        "force": False,
                        "include_detailed_response": False,
                        "max_concurrency": 25,
                        "remote_to_local_config": {"use_custom_tmp_dir": False},
                        "require_lock": False,
                        "retain_source_data": True,
                        "size_only": False,
                        "source_path": "s3://bucket/src",
                        "temporary_request_payload_path": "s3://bucket/tmp",
                    }
                ]
            },
            DemandExecutionCleanupConfigs(
                data_sync_requests=[
                    PrepareBatchDataSyncRequest(
                        source_path=S3Path("s3://bucket/src"),
                        destination_path=S3Path("s3://bucket/dst"),
                        temporary_request_payload_path=S3Path("s3://bucket/tmp"),
                    )
                ]
            ),
            does_not_raise(),
            id="Handles single PrepareBatchDataSyncRequest",
        ),
        param(
            {
                "data_sync_requests": [
                    {
                        "destination_path": "s3://bucket/dst",
                        "fail_if_missing": True,
                        "force": False,
                        "include_detailed_response": False,
                        "max_concurrency": 25,
                        "require_lock": False,
                        "retain_source_data": True,
                        "size_only": False,
                        "source_path": "s3://bucket/src",
                    }
                ]
            },
            DemandExecutionCleanupConfigs(
                data_sync_requests=[
                    PrepareBatchDataSyncRequest(
                        source_path=S3Path("s3://bucket/src"),
                        destination_path=S3Path("s3://bucket/dst"),
                    )
                ]
            ),
            does_not_raise(),
            id="Handles ambiguous ds request",
        ),
    ],
)
def test__DemandExecutionCleanupConfigs__deserialization(
    input_value, expected: DemandExecutionCleanupConfigs, raise_expectation
):
    with raise_expectation:
        actual = DemandExecutionCleanupConfigs.from_dict(input_value)
    if expected:
        assert expected == actual


@mark.parametrize(
    "input_value, expected, raise_expectation",
    [
        param(
            {
                "scratch": [
                    {
                        "file_system": "fs-11111111",
                    }
                ],
                "shared": [
                    {
                        "file_system": "fs-11111111",
                    }
                ],
                "tmp": [
                    {
                        "file_system": "fs-11111111",
                    },
                    {
                        "file_system": "fs-22222222",
                        "access_point": "fsap-22222222",
                        "container_path": "/opt/efs/tmp",
                    },
                ],
                "selection_strategy": "LEAST_UTILIZED",
            },
            DemandFileSystemConfigurations(
                scratch=[
                    FileSystemConfiguration(
                        file_system="fs-11111111",
                    )
                ],
                shared=[
                    FileSystemConfiguration(
                        file_system="fs-11111111",
                    )
                ],
                tmp=[
                    FileSystemConfiguration(
                        file_system="fs-11111111",
                    ),
                    FileSystemConfiguration(
                        file_system="fs-22222222",
                        access_point="fsap-22222222",
                        container_path="/opt/efs/tmp",
                    ),
                ],
                selection_strategy=FileSystemSelectionStrategy.LEAST_UTILIZED,
            ),
            does_not_raise(),
            id="Handles all fields present as lists",
        ),
        param(
            {
                "scratch": {
                    "file_system": "fs-11111111",
                },
                "shared": {
                    "file_system": "fs-11111111",
                },
                "tmp": {
                    "file_system": "fs-22222222",
                    "access_point": "fsap-22222222",
                    "container_path": "/opt/efs/tmp",
                },
                "selection_strategy": "LEAST_UTILIZED",
            },
            DemandFileSystemConfigurations(
                scratch=[
                    FileSystemConfiguration(
                        file_system="fs-11111111",
                    )
                ],
                shared=[
                    FileSystemConfiguration(
                        file_system="fs-11111111",
                    )
                ],
                tmp=[
                    FileSystemConfiguration(
                        file_system="fs-22222222",
                        access_point="fsap-22222222",
                        container_path="/opt/efs/tmp",
                    ),
                ],
                selection_strategy=FileSystemSelectionStrategy.LEAST_UTILIZED,
            ),
            does_not_raise(),
            id="Handles all fields present as single instances",
        ),
        param(
            {
                "scratch": [
                    {
                        "file_system": "fs-11111111",
                    }
                ],
                "shared": [
                    {
                        "file_system": "fs-11111111",
                    }
                ],
            },
            DemandFileSystemConfigurations(
                scratch=[FileSystemConfiguration(file_system="fs-11111111")],
                shared=[FileSystemConfiguration(file_system="fs-11111111")],
            ),
            does_not_raise(),
            id="Handles required fields present as lists",
        ),
        param(
            {
                "scratch": {
                    "file_system": "fs-11111111",
                },
                "shared": {
                    "file_system": "fs-11111111",
                },
            },
            DemandFileSystemConfigurations(
                scratch=[FileSystemConfiguration(file_system="fs-11111111")],
                shared=[FileSystemConfiguration(file_system="fs-11111111")],
            ),
            does_not_raise(),
            id="Handles required fields present as single instances",
        ),
        param(
            {
                "scratch": [
                    {
                        "file_system": "fs-11111111",
                    }
                ],
                "shared": {
                    "file_system": "fs-11111111",
                },
            },
            DemandFileSystemConfigurations(
                scratch=[FileSystemConfiguration(file_system="fs-11111111")],
                shared=[FileSystemConfiguration(file_system="fs-11111111")],
            ),
            does_not_raise(),
            id="Handles required fields present as mixed list and instances",
        ),
        param(
            {
                "scratch": [],
                "shared": {
                    "file_system": "fs-11111111",
                },
            },
            None,
            raises(ValueError),
            id="Raises error for empty scratch field",
        ),
    ],
)
def test__DemandFileSystemConfigurations__deserialization(
    input_value, expected: DemandFileSystemConfigurations, raise_expectation
):
    with raise_expectation:
        actual = DemandFileSystemConfigurations.from_dict(input_value)
    if expected:
        assert expected == actual
