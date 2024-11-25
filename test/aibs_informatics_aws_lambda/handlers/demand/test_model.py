from test.base import does_not_raise

from aibs_informatics_core.exceptions import ValidationError
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.email_address import EmailAddress
from pytest import mark, param, raises

from aibs_informatics_aws_lambda.handlers.demand.model import (
    DataSyncRequest,
    DemandExecutionCleanupConfigs,
    DemandExecutionSetupConfigs,
    PrepareBatchDataSyncRequest,
    PrepareDemandScaffoldingRequest,
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
                        "temporary_request_payload_path": "s3://bucket/tmp",
                    }
                ]
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
                ]
            },
            does_not_raise(),
            id="Handles single ambiguous ds request",
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
