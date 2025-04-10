from pathlib import Path
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase
from typing import Tuple, Union
from unittest import mock

from aibs_informatics_aws_utils.data_sync.file_system import Node
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.data_sync import DataSyncResult
from aibs_informatics_core.utils.time import BEGINNING_OF_TIME
from pytest import mark, param

from aibs_informatics_aws_lambda.common.handler import LambdaHandlerType
from aibs_informatics_aws_lambda.handlers.data_sync.operations import (
    DEFAULT_BUCKET_NAME_ENV_VAR,
    BatchDataSyncHandler,
    BatchDataSyncRequest,
    BatchDataSyncResponse,
    BatchDataSyncResult,
    DataSyncRequest,
    GetJSONFromFileHandler,
    GetJSONFromFileRequest,
    GetJSONFromFileResponse,
    PrepareBatchDataSyncHandler,
    PrepareBatchDataSyncRequest,
    PrepareBatchDataSyncResponse,
    PutJSONToFileHandler,
    PutJSONToFileRequest,
    PutJSONToFileResponse,
)


class GetJSONFromFileHandlerTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_download_content = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.data_sync.operations.download_to_json"
        )

    @property
    def handler(self) -> LambdaHandlerType:
        return GetJSONFromFileHandler.get_handler()

    def test__handles__valid_s3_path(self):
        s3_path = S3Path("s3://some-bucket/some-key")
        content = "hello"

        self.mock_download_content.return_value = content
        response = GetJSONFromFileResponse(content=content)
        request = GetJSONFromFileRequest(path=s3_path)
        self.assertHandles(self.handler, request.to_dict(), response.to_dict())
        self.mock_download_content.assert_called_once_with(s3_path=s3_path)

    def test__handles__valid_local_path(self):
        local_path = self.tmp_path() / "file"
        content = {}

        local_path.write_text(f"{content}")
        response = GetJSONFromFileResponse(content=content)
        request = GetJSONFromFileRequest(path=local_path)

        self.assertHandles(self.handler, request.to_dict(), response.to_dict())
        self.mock_download_content.assert_not_called()

    def test__handles__fails_on_download_error_thrown(self):
        s3_path = S3Path("s3://some-bucket/some-key")
        content = "hello"

        self.mock_download_content.side_effect = ValueError("blah")
        response = GetJSONFromFileResponse(content=content)
        request = GetJSONFromFileRequest(path=s3_path)
        with self.assertRaises(ValueError):
            self.assertHandles(self.handler, request.to_dict(), response.to_dict())

        self.mock_download_content.assert_called_once_with(s3_path=s3_path)


class PutJSONToFileHandlerTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.mock_upload_content = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.data_sync.operations.upload_json"
        )

    @property
    def handler(self) -> LambdaHandlerType:
        return PutJSONToFileHandler.get_handler()

    def test__handles__puts_content_with_s3_path_specified(self):
        s3_path = S3Path("s3://some-bucket/some-key")
        content = "hello"

        request = PutJSONToFileRequest(content=content, path=s3_path)
        response = PutJSONToFileResponse(path=s3_path)

        self.assertHandles(self.handler, request.to_dict(), response.to_dict())

        self.mock_upload_content.assert_called_once_with(
            content, s3_path=s3_path, extra_args=mock.ANY
        )

    def test__handles__puts_content_with_local_path_specified(self):
        path = self.tmp_path() / "file"
        content = {}

        request = PutJSONToFileRequest(content=content, path=path)
        response = PutJSONToFileResponse(path=path)

        self.assertHandles(self.handler, request.to_dict(), response.to_dict())

        assert isinstance(response.path, Path)
        assert response.path.exists()
        assert response.path.read_text() == f"{content}"

        self.mock_upload_content.assert_not_called()

    def test__handles__fails_if_no_path_provided_and_env_var_not_set(self):
        s3_path = S3Path("s3://some-bucket/some-key")
        content = "hello"
        request = PutJSONToFileRequest(content=content)

        self.assertLambdaRaises(self.handler, request.to_dict(), ValueError)

        self.mock_upload_content.assert_not_called()

    def test__handles__uploads_content_with_no_path_specified(self):
        s3_path = S3Path(
            "s3://some-bucket/scratch/12345678-1234-1234-1234-123456789012/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )
        content = "hello"
        request = PutJSONToFileRequest(content=content)
        response = PutJSONToFileResponse(path=s3_path)
        self.set_env_vars((DEFAULT_BUCKET_NAME_ENV_VAR, "some-bucket"))
        self.assertHandles(self.handler, request.to_dict(), response.to_dict())

        self.mock_upload_content.assert_called_once_with(
            content, s3_path=s3_path, extra_args=mock.ANY
        )


class PrepareBatchDataSyncHandlerTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.mock_upload_content = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.data_sync.operations.upload_json"
        )

    @property
    def handler(self) -> LambdaHandlerType:
        return PrepareBatchDataSyncHandler.get_handler()

    def test__batch_nodes__handles_unordered_list_of_nodes(self):
        n1 = self.create_node("a", 6)
        n2 = self.create_node("b", 7)
        n3 = self.create_node("c", 3)
        n4 = self.create_node("d", 2)

        nodes = [n1, n2, n3, n4]
        expected_node_batches = [[n2, n3], [n1, n4]]

        node_batches = PrepareBatchDataSyncHandler.build_node_batches(nodes, 10)
        self.assertEqual(expected_node_batches, node_batches)

    def test__batch_nodes__handles_nodes_greater_than_limit(self):
        n1 = self.create_node("a", 11)
        n2 = self.create_node("b", 6)
        n3 = self.create_node("c", 7)
        n4 = self.create_node("d", 3)
        n5 = self.create_node("d", 2)

        nodes = [n1, n2, n3, n4, n5]
        expected_node_batches = [[n1], [n3, n4], [n2, n5]]

        node_batches = PrepareBatchDataSyncHandler.build_node_batches(nodes, 10)
        self.assertEqual(expected_node_batches, node_batches)

    def test__handle__prepare_local_to_s3__simple(self):
        fs = self.setUpLocalFS(
            ("a", 1),
            ("b", 1),
            ("c", 1),
        )
        source_path = fs
        destination_path = S3Path.build(bucket_name="bucket", key="key/")
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=destination_path,
            batch_size_bytes_limit=10,
            max_concurrency=10,
            retain_source_data=True,
        )
        expected = PrepareBatchDataSyncResponse(
            requests=[
                BatchDataSyncRequest(
                    requests=[
                        DataSyncRequest(
                            source_path=source_path,
                            destination_path=destination_path,
                            max_concurrency=10,
                            retain_source_data=True,
                        )
                    ]
                )
            ]
        )
        self.assertHandles(self.handler, request.to_dict(), expected.to_dict())

    def test__handle__prepare_local_to_s3__simple__non_default_args_preserved(self):
        fs = self.setUpLocalFS(
            ("a", 1),
            ("b", 1),
            ("c", 1),
        )
        source_path = fs
        destination_path = S3Path.build(bucket_name="bucket", key="key/")
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=destination_path,
            batch_size_bytes_limit=10,
            max_concurrency=10,
            retain_source_data=True,
            size_only=True,
            force=True,
            include_detailed_response=True,
        )
        expected = PrepareBatchDataSyncResponse(
            requests=[
                BatchDataSyncRequest(
                    requests=[
                        DataSyncRequest(
                            source_path=source_path,
                            destination_path=destination_path,
                            max_concurrency=10,
                            retain_source_data=True,
                            size_only=True,
                            force=True,
                            include_detailed_response=True,
                        )
                    ]
                )
            ]
        )
        self.assertHandles(self.handler, request.to_dict(), expected.to_dict())

    def test__handle__prepare_local_to_s3__simple__upload_to_s3(self):
        fs = self.setUpLocalFS(
            ("a", 1),
            ("b", 1),
            ("c", 1),
        )
        source_path = fs
        destination_path = S3Path.build(bucket_name="bucket", key="key/")
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=destination_path,
            batch_size_bytes_limit=10,
            max_concurrency=10,
            retain_source_data=True,
            temporary_request_payload_path=S3Path.build(bucket_name="bucket", key="intermediate/"),
        )

        expected_json = [
            DataSyncRequest(
                source_path=source_path,
                destination_path=destination_path,
                max_concurrency=10,
                retain_source_data=True,
            ).to_dict()
        ]
        expected = PrepareBatchDataSyncResponse(
            requests=[
                BatchDataSyncRequest(
                    requests=S3Path.build(bucket_name="bucket", key="intermediate/request_0.json"),
                )
            ]
        )
        self.assertHandles(self.handler, request.to_dict(), expected.to_dict())

        self.mock_upload_content.assert_called_once_with(
            expected_json,
            s3_path=S3Path.build(bucket_name="bucket", key="intermediate/request_0.json"),
        )

    def test__handle__prepare_local_to_s3__complex(self):
        fs = self.setUpLocalFS(
            ("a", 3),
            ("b", 7),
            ("c", 10),
        )
        source_path = fs
        destination_path = S3Path.build(bucket_name="bucket", key="key/")
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=destination_path,
            batch_size_bytes_limit=10,
            max_concurrency=10,
            retain_source_data=True,
        )
        expected = PrepareBatchDataSyncResponse(
            requests=[
                BatchDataSyncRequest(
                    requests=[
                        DataSyncRequest(
                            source_path=source_path / "c",
                            destination_path=destination_path + "c",
                            max_concurrency=10,
                            retain_source_data=True,
                        )
                    ]
                ),
                BatchDataSyncRequest(
                    requests=[
                        DataSyncRequest(
                            source_path=source_path / "a",
                            destination_path=destination_path + "a",
                            max_concurrency=10,
                            retain_source_data=True,
                        ),
                        DataSyncRequest(
                            source_path=source_path / "b",
                            destination_path=destination_path + "b",
                            max_concurrency=10,
                            retain_source_data=True,
                        ),
                    ]
                ),
            ]
        )
        self.assertHandles(self.handler, request.to_dict(), expected.to_dict())

    def test__handle__prepare_local_to_local__complex(self):
        fs = self.setUpLocalFS(
            ("src/a", 3),
            ("src/b", 7),
            ("src/c", 10),
        )
        source_path = fs / "src"
        destination_path = fs / "dst"

        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=destination_path,
            batch_size_bytes_limit=10,
            max_concurrency=10,
            retain_source_data=True,
        )
        expected = PrepareBatchDataSyncResponse(
            requests=[
                BatchDataSyncRequest(
                    requests=[
                        DataSyncRequest(
                            source_path=source_path / "c",
                            destination_path=destination_path / "c",
                            max_concurrency=10,
                            retain_source_data=True,
                        )
                    ]
                ),
                BatchDataSyncRequest(
                    requests=[
                        DataSyncRequest(
                            source_path=source_path / "a",
                            destination_path=destination_path / "a",
                            max_concurrency=10,
                            retain_source_data=True,
                        ),
                        DataSyncRequest(
                            source_path=source_path / "b",
                            destination_path=destination_path / "b",
                            max_concurrency=10,
                            retain_source_data=True,
                        ),
                    ]
                ),
            ]
        )
        self.assertHandles(self.handler, request.to_dict(), expected.to_dict())

    def test__build__build_destination_path__s3_to_s3__single_folder(self):
        request = PrepareBatchDataSyncRequest(
            source_path=Path("/scratch/dir"),
            destination_path=S3Path("s3://bucket2/scratch/dir"),
            batch_size_bytes_limit=10,
            max_concurrency=10,
            retain_source_data=True,
        )
        expected = "s3://bucket2/scratch/dir/"
        node = Node("/scratch/dir", None)
        node.add_object("file", 1, BEGINNING_OF_TIME)
        actual = PrepareBatchDataSyncHandler.build_destination_path(request, node)
        self.assertEqual(expected, actual)

    def setUpLocalFS(self, *paths: Tuple[Union[Path, str], int]) -> Path:
        root_file_system = self.tmp_path()
        for relative_path, size in paths:
            full_path = root_file_system / relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text("0" * size)
        return root_file_system

    def create_node(self, key: str, size_bytes: int = 1) -> Node:
        return Node(key, size_bytes=size_bytes, object_count=1)


class BatchDataSyncHandlerTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_sync_operations = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.data_sync.operations.DataSyncOperations.sync"
        )
        self.mock_download_to_json = self.create_patch(
            "aibs_informatics_aws_lambda.handlers.data_sync.operations.download_to_json"
        )

    @property
    def handler(self) -> LambdaHandlerType:
        return BatchDataSyncHandler.get_handler()

    def test__handle__list_of_paths__all_succeed(self):
        fs = self.setUpLocalFS(
            ("src/a", 3),
            ("src/b", 7),
            ("src/c", 10),
        )
        source_path = fs / "src"
        destination_path = fs / "dst"

        requests = [
            DataSyncRequest(
                source_path=source_path / "a",
                destination_path=destination_path / "a",
                max_concurrency=10,
                retain_source_data=True,
            ),
            DataSyncRequest(
                source_path=source_path / "b",
                destination_path=destination_path / "b",
                max_concurrency=10,
                retain_source_data=True,
            ),
            DataSyncRequest(
                source_path=source_path / "c",
                destination_path=destination_path / "c",
                max_concurrency=10,
                retain_source_data=True,
            ),
        ]

        self.mock_sync_operations.side_effect = [
            DataSyncResult(files_transferred=1, bytes_transferred=3),
            DataSyncResult(files_transferred=1, bytes_transferred=7),
            DataSyncResult(files_transferred=1, bytes_transferred=10),
        ]

        batch_request = BatchDataSyncRequest(requests=requests)
        response = BatchDataSyncResponse(
            result=BatchDataSyncResult(
                total_requests_count=3,
                successful_requests_count=3,
                failed_requests_count=0,
                bytes_transferred=20,
                files_transferred=3,
            ),
            failed_requests=[],
        )

        self.assertHandles(self.handler, batch_request.to_dict(), response.to_dict())

        self.mock_download_to_json.assert_not_called()
        self.mock_sync_operations.assert_called()
        self.assertEqual(self.mock_sync_operations.call_count, 3)

    def test__handle__list_of_paths__partial_success(self):
        fs = self.setUpLocalFS(
            ("src/a", 3),
            ("src/b", 7),
            ("src/c", 10),
        )
        source_path = fs / "src"
        destination_path = fs / "dst"

        requests = [
            DataSyncRequest(
                source_path=source_path / "a",
                destination_path=destination_path / "a",
                max_concurrency=10,
                retain_source_data=True,
            ),
            DataSyncRequest(
                source_path=source_path / "b",
                destination_path=destination_path / "b",
                max_concurrency=10,
                retain_source_data=True,
            ),
            DataSyncRequest(
                source_path=source_path / "c",
                destination_path=destination_path / "c",
                max_concurrency=10,
                retain_source_data=True,
            ),
        ]

        self.mock_sync_operations.side_effect = [
            DataSyncResult(files_transferred=1, bytes_transferred=3),
            ValueError("Sync failed"),
            DataSyncResult(files_transferred=1, bytes_transferred=10),
        ]

        batch_request = BatchDataSyncRequest(
            requests=requests,
            allow_partial_failure=True,
        )
        response = BatchDataSyncResponse(
            result=BatchDataSyncResult(
                total_requests_count=3,
                successful_requests_count=2,
                failed_requests_count=1,
                bytes_transferred=13,
                files_transferred=2,
            ),
            failed_requests=[requests[1]],
        )

        self.assertHandles(self.handler, batch_request.to_dict(), response.to_dict())

        self.mock_download_to_json.assert_not_called()
        self.mock_sync_operations.assert_called()
        self.assertEqual(self.mock_sync_operations.call_count, 3)

    def test__handle__list_of_paths__fails(self):
        fs = self.setUpLocalFS(
            ("src/a", 3),
            ("src/b", 7),
            ("src/c", 10),
        )
        source_path = fs / "src"
        destination_path = fs / "dst"

        requests = [
            DataSyncRequest(
                source_path=source_path / "a",
                destination_path=destination_path / "a",
                max_concurrency=10,
                retain_source_data=True,
            ),
            DataSyncRequest(
                source_path=source_path / "b",
                destination_path=destination_path / "b",
                max_concurrency=10,
                retain_source_data=True,
            ),
            DataSyncRequest(
                source_path=source_path / "c",
                destination_path=destination_path / "c",
                max_concurrency=10,
                retain_source_data=True,
            ),
        ]

        self.mock_sync_operations.side_effect = [
            DataSyncResult(files_transferred=1, bytes_transferred=3),
            ValueError("Sync failed"),
            DataSyncResult(files_transferred=1, bytes_transferred=10),
        ]

        batch_request = BatchDataSyncRequest(
            requests=requests,
            allow_partial_failure=False,
        )

        self.assertLambdaRaises(self.handler, batch_request.to_dict(), ValueError)

        self.mock_download_to_json.assert_not_called()
        self.mock_sync_operations.assert_called()
        self.assertEqual(self.mock_sync_operations.call_count, 2)

    def test__handle__requests_stored_in_s3(self):
        s3_path = S3Path("s3://bucket/intermediate/request_0.json")
        content = [
            DataSyncRequest(
                source_path=Path("/src/a"),
                destination_path=Path("/dst/a"),
                max_concurrency=10,
                retain_source_data=True,
            ).to_dict(),
        ]

        self.mock_download_to_json.return_value = content
        self.mock_sync_operations.return_value = DataSyncResult(
            files_transferred=1, bytes_transferred=3
        )
        batch_request = BatchDataSyncRequest(requests=s3_path)
        response = BatchDataSyncResponse(
            result=BatchDataSyncResult(
                total_requests_count=1,
                successful_requests_count=1,
                failed_requests_count=0,
                bytes_transferred=3,
                files_transferred=1,
            ),
            failed_requests=[],
        )

        self.assertHandles(self.handler, batch_request.to_dict(), response.to_dict())

        self.mock_download_to_json.assert_called_once_with(s3_path)
        self.mock_sync_operations.assert_called()
        self.assertEqual(self.mock_sync_operations.call_count, 1)

    def setUpLocalFS(self, *paths: Tuple[Union[Path, str], int]) -> Path:
        root_file_system = self.tmp_path()
        for relative_path, size in paths:
            full_path = root_file_system / relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text("0" * size)
        return root_file_system


@mark.parametrize(
    "request_obj, node, expected",
    [
        param(
            PrepareBatchDataSyncRequest(
                source_path=Path("/scratch/dir"),
                destination_path=S3Path("s3://bucket2/prefix"),
            ),
            Node("/scratch/dir", children={"a": Node("a")}),
            S3Path("s3://bucket2/prefix/"),
            id="local to s3 root node as folder adds separator",
        ),
        param(
            PrepareBatchDataSyncRequest(
                source_path=Path("/scratch/dir"),
                destination_path=S3Path("s3://bucket2/prefix/"),
            ),
            Node("/scratch/dir", children={"a": Node("a")}),
            S3Path("s3://bucket2/prefix/"),
            id="local to s3 root node as folder does not add separator when prefix has it",
        ),
        param(
            PrepareBatchDataSyncRequest(
                source_path=Path("/scratch/obj"),
                destination_path=S3Path("s3://bucket2/prefix"),
            ),
            Node("/scratch/obj"),
            S3Path("s3://bucket2/prefix"),
            id="local to s3 root node as file does not add separator",
        ),
        param(
            PrepareBatchDataSyncRequest(
                source_path=S3Path("s3://bucket/scratch/dir"),
                destination_path=S3Path("s3://bucket2/prefix"),
            ),
            Node("scratch/dir", children={"a": Node("a")}),
            S3Path("s3://bucket2/prefix/"),
            id="s3 to s3 root node as folder",
        ),
        param(
            PrepareBatchDataSyncRequest(
                source_path=S3Path("s3://bucket/scratch/obj"),
                destination_path=S3Path("s3://bucket2/prefix"),
            ),
            Node("scratch/obj"),
            S3Path("s3://bucket2/prefix"),
            id="s3 to s3 root node as file",
        ),
        param(
            PrepareBatchDataSyncRequest(
                source_path=S3Path("s3://bucket/scratch/obj"),
                destination_path=Path("/scratch/abc"),
            ),
            Node("scratch/obj"),
            Path("/scratch/abc"),
            id="s3 to local root node as file",
        ),
        param(
            PrepareBatchDataSyncRequest(
                source_path=S3Path("s3://bucket/scratch/dir"),
                destination_path=Path("/scratch/abc"),
            ),
            Node("scratch/dir", children={"a": Node("a")}),
            Path("/scratch/abc"),
            id="s3 to local root node as prefix (no sep)",
        ),
        param(
            PrepareBatchDataSyncRequest(
                source_path=S3Path("s3://bucket/scratch/dir/"),
                destination_path=Path("/scratch/abc"),
            ),
            Node("scratch/dir/", children={"a": Node("a")}),
            Path("/scratch/abc"),
            id="s3 to local root node as prefix (with sep)",
        ),
    ],
)
def test__PrepareBatchDataSyncHandler_build_destination_path(
    request_obj: PrepareBatchDataSyncRequest,
    node: Node,
    expected: Union[Path, S3Path],
):
    actual = PrepareBatchDataSyncHandler.build_destination_path(request_obj, node)

    assert actual == expected
