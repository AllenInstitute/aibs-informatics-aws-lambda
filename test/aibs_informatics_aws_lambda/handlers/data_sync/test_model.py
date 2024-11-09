from pathlib import Path

from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3Path

from aibs_informatics_aws_lambda.handlers.data_sync.model import ListDataPathsRequest, WithDataPath


def test_with_data_path_efs():
    efs_path = EFSPath("fs-123456789:/efs")
    with_data_path_efs = WithDataPath(path=efs_path)
    assert with_data_path_efs.efs_path == efs_path
    assert with_data_path_efs.s3_uri is None
    assert with_data_path_efs.local_path is None


def test_with_data_path_s3():
    s3_path = S3Path("s3://bucket/key")
    with_data_path_s3 = WithDataPath(path=s3_path)
    assert with_data_path_s3.efs_path is None
    assert with_data_path_s3.s3_uri == s3_path
    assert with_data_path_s3.local_path is None


def test_with_data_path_local():
    local_path = Path("/local/path")
    with_data_path_local = WithDataPath(path=local_path)
    assert with_data_path_local.efs_path is None
    assert with_data_path_local.s3_uri is None
    assert with_data_path_local.local_path == local_path


def test_list_data_paths_request_single_include_pattern():
    request = ListDataPathsRequest(path="/some/path", include=".*\\.txt")
    assert request.include_patterns is not None
    assert len(request.include_patterns) == 1
    assert request.include_patterns[0].pattern == ".*\\.txt"


def test_list_data_paths_request_single_exclude_pattern():
    request = ListDataPathsRequest(path="/some/path", exclude=".*\\.log")
    assert request.exclude_patterns is not None
    assert len(request.exclude_patterns) == 1
    assert request.exclude_patterns[0].pattern == ".*\\.log"


def test_list_data_paths_request_multiple_include_patterns():
    request = ListDataPathsRequest(path="/some/path", include=[".*\\.txt", ".*\\.csv"])
    assert request.include_patterns is not None
    assert len(request.include_patterns) == 2
    assert request.include_patterns[0].pattern == ".*\\.txt"
    assert request.include_patterns[1].pattern == ".*\\.csv"


def test_list_data_paths_request_multiple_exclude_patterns():
    request = ListDataPathsRequest(path="/some/path", exclude=[".*\\.log", ".*\\.tmp"])
    assert request.exclude_patterns is not None
    assert len(request.exclude_patterns) == 2
    assert request.exclude_patterns[0].pattern == ".*\\.log"
    assert request.exclude_patterns[1].pattern == ".*\\.tmp"
