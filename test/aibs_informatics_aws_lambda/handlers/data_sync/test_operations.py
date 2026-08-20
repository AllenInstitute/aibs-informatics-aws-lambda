from pathlib import Path
from typing import cast
from unittest import mock

from aibs_informatics_aws_utils.data_sync.file_system import Node
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.data_sync import DataSyncFilterConfig, DataSyncResult
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
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase


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
                            filter_root=str(source_path),
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
                            filter_root=str(source_path),
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
                filter_root=str(source_path),
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
                            filter_root=str(source_path),
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
                            filter_root=str(source_path),
                            max_concurrency=10,
                            retain_source_data=True,
                        ),
                        DataSyncRequest(
                            source_path=source_path / "b",
                            destination_path=destination_path + "b",
                            filter_root=str(source_path),
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
                            filter_root=str(source_path),
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
                            filter_root=str(source_path),
                            max_concurrency=10,
                            retain_source_data=True,
                        ),
                        DataSyncRequest(
                            source_path=source_path / "b",
                            destination_path=destination_path / "b",
                            filter_root=str(source_path),
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

    # -----------------------------------------------------------------------
    # Filtering (OCSDV-452)
    # -----------------------------------------------------------------------

    def setUpFilterableFS(self) -> Path:
        """Two samples, each one large .bam (10B) plus one small .txt (1B).

        Total is 22 bytes; the .bam-only subset is 20. With a 10 byte batch limit
        that difference is what separates binning on kept bytes from binning on
        total bytes -- see the partition test below.
        """
        return self.setUpLocalFS(
            ("sampleA/reads.bam", 10),
            ("sampleA/notes.txt", 1),
            ("sampleB/reads.bam", 10),
            ("sampleB/notes.txt", 1),
        )

    def prepare_sub_requests(
        self,
        source_path: Path,
        destination_path: S3Path,
        filter_config: DataSyncFilterConfig | None,
        batch_size_bytes_limit: int = 10,
        **kwargs,
    ) -> list[DataSyncRequest]:
        """Run the handler and return the flattened sub-requests it emitted."""
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=destination_path,
            batch_size_bytes_limit=batch_size_bytes_limit,
            filter_config=filter_config,
            **kwargs,
        )
        response = PrepareBatchDataSyncResponse.from_dict(
            self.handler(request.to_dict(), self.context)
        )
        return [
            r for batch in response.requests for r in cast(list[DataSyncRequest], batch.requests)
        ]

    def sub_request_sources(self, *args, **kwargs) -> list[str]:
        """The sorted source paths of the sub-requests the handler emitted."""
        return sorted(str(r.source_path) for r in self.prepare_sub_requests(*args, **kwargs))

    def test__handle__partition_bins_on_kept_bytes_not_total(self):
        """Filters must be applied while building the tree, not after.

        Unfiltered, every sample directory (11B) exceeds the 10B limit, so the
        partition descends to individual files and emits one sub-request per
        file. Filtered, each sample directory holds only its 10B .bam and fits,
        so the partition stops at the directory. Binning on total bytes would
        produce the unfiltered shape and every batch job would run near-empty.
        """
        source_path = self.setUpFilterableFS()
        destination_path = S3Path.build(bucket_name="bucket", key="key/")

        self.assertEqual(
            self.sub_request_sources(source_path, destination_path, None),
            [
                f"{source_path}/sampleA/notes.txt",
                f"{source_path}/sampleA/reads.bam",
                f"{source_path}/sampleB/notes.txt",
                f"{source_path}/sampleB/reads.bam",
            ],
        )
        self.assertEqual(
            self.sub_request_sources(
                source_path, destination_path, DataSyncFilterConfig(include=r".*\.bam")
            ),
            [f"{source_path}/sampleA", f"{source_path}/sampleB"],
        )

    def test__handle__every_sub_request_carries_patterns_and_original_filter_root(self):
        """Design decision 2: identical shape on every sub-request.

        Each sub-request is rooted at a sub-prefix and re-lists from there, so
        without the original root riding along, a pattern written against the
        original root stops matching. No node is special-cased on what it
        matched -- all carry the same patterns, root, and delete flag.
        """
        source_path = self.setUpFilterableFS()
        # Both .bam files are kept (20B), which exceeds the 10B limit and so
        # forces a split into per-sample sub-prefixes -- the case where anchoring
        # actually matters.
        filter_config = DataSyncFilterConfig(include=r".*\.bam")
        sub_requests = self.prepare_sub_requests(
            source_path,
            S3Path.build(bucket_name="bucket", key="key/"),
            filter_config,
            delete=False,
        )

        self.assertEqual(
            sorted(str(r.source_path) for r in sub_requests),
            [f"{source_path}/sampleA", f"{source_path}/sampleB"],
        )
        for sub_request in sub_requests:
            # Rooted at a sub-prefix ...
            self.assertNotEqual(str(sub_request.source_path), str(source_path))
            # ... but anchored to the ORIGINAL root, with the original patterns.
            self.assertEqual(sub_request.filter_root, str(source_path))
            self.assertEqual(sub_request.filter_config, filter_config)
            self.assertEqual(sub_request.delete, False)

    def test__handle__inbound_filter_root_is_preserved(self):
        """An explicitly supplied filter_root wins over the source path.

        The tree we bin on and the sub-requests we emit must anchor identically,
        so an inbound root has to reach both.
        """
        source_path = self.setUpFilterableFS()
        sub_requests = self.prepare_sub_requests(
            source_path,
            S3Path.build(bucket_name="bucket", key="key/"),
            # Anchored at the parent, so it only matches if the tree was built
            # against filter_root. Against source_path it matches nothing and the
            # handler raises on zero matches -- which is what makes this a real
            # anchoring assertion rather than just a propagation one.
            DataSyncFilterConfig(include=rf"{source_path.name}/.*\.bam"),
            filter_root=str(source_path.parent),
        )

        self.assertTrue(sub_requests)
        for sub_request in sub_requests:
            self.assertEqual(sub_request.filter_root, str(source_path.parent))

    def test__handle__zero_matches__raises_and_names_patterns(self):
        """Design decision 5 -- fail before any Batch job launches.

        A typo'd pattern otherwise yields a green execution over an empty input
        set, so the message has to name the patterns that produced it.
        """
        source_path = self.setUpFilterableFS()
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=S3Path.build(bucket_name="bucket", key="key/"),
            batch_size_bytes_limit=10,
            filter_config=DataSyncFilterConfig(include=r".*\.cram"),
            fail_if_missing=True,
        )

        with self.assertRaisesRegex(ValueError, r"\.cram"):
            self.handler(request.to_dict(), self.context)

        # Nothing was staged for upload before the failure.
        self.mock_upload_content.assert_not_called()

    def test__handle__zero_matches__warns_when_not_fail_if_missing(self):
        """fail_if_missing is the gate -- without it, a zero match only warns.

        The tree is empty, so the partition yields the (empty) root and a single
        sub-request covering it. That sub-request still carries the filters, so
        the downstream sync matches nothing too and transfers nothing.
        """
        source_path = self.setUpFilterableFS()
        filter_config = DataSyncFilterConfig(include=r".*\.cram")
        sub_requests = self.prepare_sub_requests(
            source_path,
            S3Path.build(bucket_name="bucket", key="key/"),
            filter_config,
            fail_if_missing=False,
        )
        self.assertEqual([str(r.source_path) for r in sub_requests], [str(source_path)])
        self.assertEqual(sub_requests[0].filter_config, filter_config)
        self.assertEqual(sub_requests[0].filter_root, str(source_path))

    def test__handle__filtered_local_to_local__raises_before_emitting_requests(self):
        """Fail once here rather than N times inside the fan-out.

        sync_local_to_local cannot apply filters and raises when given any, so
        emitting sub-requests for one would just distribute the same failure across
        every Batch job.
        """
        source_path = self.setUpFilterableFS()
        destination_path = self.tmp_path() / "destination"
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=destination_path,
            batch_size_bytes_limit=10,
            filter_config=DataSyncFilterConfig(include=r".*\.bam"),
        )

        with self.assertRaisesRegex(ValueError, "not supported for local -> local"):
            self.handler(request.to_dict(), self.context)

        self.mock_upload_content.assert_not_called()

    def test__handle__unfiltered_local_to_local__is_allowed(self):
        source_path = self.setUpFilterableFS()
        destination_path = self.tmp_path() / "destination"

        self.assertEqual(
            self.sub_request_sources(source_path, destination_path, None),
            [
                f"{source_path}/sampleA/notes.txt",
                f"{source_path}/sampleA/reads.bam",
                f"{source_path}/sampleB/notes.txt",
                f"{source_path}/sampleB/reads.bam",
            ],
        )

    def test__handle__filtered_local_to_s3__is_allowed(self):
        source_path = self.setUpFilterableFS()

        sub_requests = self.prepare_sub_requests(
            source_path,
            S3Path.build(bucket_name="bucket", key="key/"),
            DataSyncFilterConfig(include=r".*\.bam"),
        )

        self.assertTrue(sub_requests)

    def test__handle__empty_source__is_not_a_zero_match_failure(self):
        """An empty source is the pre-existing "missing source" case, not this one."""
        source_path = self.tmp_path() / "empty"
        source_path.mkdir(parents=True, exist_ok=True)
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=S3Path.build(bucket_name="bucket", key="key/"),
            batch_size_bytes_limit=10,
            filter_config=DataSyncFilterConfig(include=r".*\.bam"),
            fail_if_missing=True,
        )

        response = PrepareBatchDataSyncResponse.from_dict(
            self.handler(request.to_dict(), self.context)
        )
        self.assertEqual(len(response.requests), 1)

    def test__handle__response_round_trips_through_dict_preserving_filters(self):
        """The seam the Step Functions Map state crosses.

        The prepare handler's response is serialized to JSON, handed to a Map
        state, and each item deserialized back into a request inside a separate
        Batch job. Ordinary unit tests hold the objects in memory and never
        exercise that boundary -- so a field that serializes lossily would look
        fine everywhere except production.
        """
        source_path = self.setUpFilterableFS()
        # Both list-valued, and sized so the response spans more than one
        # sub-request -- the Map state fans out over exactly this list.
        filter_config = DataSyncFilterConfig(include=[r".*\.bam"], exclude=[r".*/notes\.txt"])
        request = PrepareBatchDataSyncRequest(
            source_path=source_path,
            destination_path=S3Path.build(bucket_name="bucket", key="key/"),
            batch_size_bytes_limit=10,
            filter_config=filter_config,
        )

        response = PrepareBatchDataSyncResponse.from_dict(
            self.handler(request.to_dict(), self.context)
        )

        serialized = response.to_dict()
        restored = PrepareBatchDataSyncResponse.from_dict(serialized)

        self.assertEqual(restored, response)

        # Assert on the serialized form too: from_dict alone would re-default a
        # dropped field back to something that compares equal to an unset one.
        serialized_sub_requests = [
            r
            for batch in cast(list[dict], serialized["requests"])
            for r in cast(list[dict], batch["requests"])
        ]
        self.assertEqual(len(serialized_sub_requests), 2)
        for serialized_sub_request in serialized_sub_requests:
            self.assertEqual(
                serialized_sub_request["filter_config"],
                {"include": [r".*\.bam"], "exclude": [r".*/notes\.txt"]},
            )
            self.assertEqual(serialized_sub_request["filter_root"], str(source_path))

        restored_sub_requests = [
            r for batch in restored.requests for r in cast(list[DataSyncRequest], batch.requests)
        ]
        self.assertTrue(restored_sub_requests)
        for restored_sub_request in restored_sub_requests:
            self.assertEqual(restored_sub_request.filter_config, filter_config)
            self.assertEqual(restored_sub_request.filter_root, str(source_path))

    def setUpLocalFS(self, *paths: tuple[Path | str, int]) -> Path:
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
            "aibs_informatics_aws_lambda.handlers.data_sync.operations.DataSyncOperations.sync_task"
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

    def test__handle__forwards_filters_to_sync(self):
        """This handler executes the sub-requests prepare emits.

        It used to enumerate task fields by hand when calling into the sync
        operations, which would have dropped filter_config/filter_root and made
        filtering a silent no-op across the whole distributed workflow.
        """
        filter_config = DataSyncFilterConfig(include=r".*\.bam")
        request = DataSyncRequest(
            source_path=Path("/src/sampleA"),
            destination_path=Path("/dst/sampleA"),
            filter_config=filter_config,
            filter_root="/src",
        )

        self.mock_sync_operations.return_value = DataSyncResult(
            files_transferred=1, bytes_transferred=3
        )

        self.handler(BatchDataSyncRequest(requests=[request]).to_dict(), self.context)

        self.mock_sync_operations.assert_called_once()
        (task,) = self.mock_sync_operations.call_args.args
        self.assertEqual(task.filter_config, filter_config)
        self.assertEqual(task.filter_root, "/src")

    def setUpLocalFS(self, *paths: tuple[Path | str, int]) -> Path:
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
    expected: Path | S3Path,
):
    actual = PrepareBatchDataSyncHandler.build_destination_path(request_obj, node)

    assert actual == expected
