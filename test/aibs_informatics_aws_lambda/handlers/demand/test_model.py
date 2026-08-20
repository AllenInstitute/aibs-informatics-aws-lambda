from aibs_informatics_core.exceptions import ValidationError
from aibs_informatics_core.models.aws.s3 import S3Path
from pytest import mark, param, raises

from aibs_informatics_aws_lambda.handlers.data_sync.model import RemoveDataPathsRequest
from aibs_informatics_aws_lambda.handlers.demand.model import (
    DemandExecutionCleanupConfigs,
    DemandFileSystemConfigurations,
    FileSystemConfiguration,
    FileSystemSelectionStrategy,
    PrepareBatchDataSyncRequest,
)
from test.base import does_not_raise


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
                        "delete": True,
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
                        "delete": True,
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


def test__DemandFileSystemConfigurations__defaults_preserve_legacy_behavior():
    configs = DemandFileSystemConfigurations()
    assert configs.shared == [FileSystemConfiguration()]
    assert configs.scratch == [FileSystemConfiguration()]
    assert configs.tmp == []
    assert configs.selection_strategy == FileSystemSelectionStrategy.RANDOM


def test__DemandFileSystemConfigurations__coerces_legacy_singular_shape():
    configs = DemandFileSystemConfigurations.from_dict(
        {
            "shared": {"file_system": "fs-shared"},
            "scratch": {"file_system": "fs-scratch"},
            "tmp": {"file_system": "fs-tmp"},
        }
    )
    assert configs.shared == [FileSystemConfiguration(file_system="fs-shared")]
    assert configs.scratch == [FileSystemConfiguration(file_system="fs-scratch")]
    assert configs.tmp == [FileSystemConfiguration(file_system="fs-tmp")]


def test__DemandFileSystemConfigurations__accepts_null_tmp():
    configs = DemandFileSystemConfigurations.from_dict(
        {
            "shared": {"file_system": "fs-shared"},
            "scratch": {"file_system": "fs-scratch"},
            "tmp": None,
        }
    )
    assert configs.tmp == []


def test__DemandFileSystemConfigurations__accepts_candidate_lists():
    configs = DemandFileSystemConfigurations.from_dict(
        {
            "shared": [{"file_system": "fs-shared"}],
            "scratch": [
                {"file_system": "fs-scratch-1"},
                {"file_system": "fs-scratch-2"},
            ],
            "selection_strategy": "RANDOM",
        }
    )
    assert configs.shared == [FileSystemConfiguration(file_system="fs-shared")]
    assert configs.scratch == [
        FileSystemConfiguration(file_system="fs-scratch-1"),
        FileSystemConfiguration(file_system="fs-scratch-2"),
    ]
    assert configs.selection_strategy == FileSystemSelectionStrategy.RANDOM


@mark.parametrize("field_name", ["shared", "scratch"])
def test__DemandFileSystemConfigurations__rejects_empty_required_roles(field_name):
    with raises(
        ValidationError, match=f"At least one {field_name} file system config is required"
    ):
        DemandFileSystemConfigurations.from_dict({field_name: []})


def test__DemandFileSystemConfigurations__serializes_as_lists():
    configs = DemandFileSystemConfigurations.from_dict(
        {
            "shared": {"file_system": "fs-shared"},
            "scratch": [{"file_system": "fs-scratch-1"}, {"file_system": "fs-scratch-2"}],
        }
    )
    data = configs.to_dict()
    assert data["shared"] == [{"file_system": "fs-shared"}]
    assert data["scratch"] == [
        {"file_system": "fs-scratch-1"},
        {"file_system": "fs-scratch-2"},
    ]
    assert data["selection_strategy"] == "RANDOM"
