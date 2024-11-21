import json
from pathlib import Path
from typing import List, Optional, Union, cast

from aibs_informatics_aws_utils.data_sync import (
    DataSyncOperations,
    LocalFileSystem,
    Node,
    S3FileSystem,
)
from aibs_informatics_aws_utils.s3 import SCRATCH_EXTRA_ARGS, download_to_json, upload_json
from aibs_informatics_core.models.aws.s3 import S3URI, S3Key
from aibs_informatics_core.models.data_sync import (
    BatchDataSyncRequest,
    BatchDataSyncResponse,
    BatchDataSyncResult,
    DataSyncRequest,
    DataSyncResponse,
    GetJSONFromFileRequest,
    GetJSONFromFileResponse,
    PrepareBatchDataSyncRequest,
    PrepareBatchDataSyncResponse,
    PutJSONToFileRequest,
    PutJSONToFileResponse,
)
from aibs_informatics_core.models.unique_ids import UniqueID
from aibs_informatics_core.utils.hashing import sha256_hexdigest
from aibs_informatics_core.utils.json import JSON, load_json
from aibs_informatics_core.utils.os_operations import get_env_var
from aibs_informatics_core.utils.tools.strtools import removeprefix
from aibs_informatics_core.utils.units import BYTES_PER_GIBIBYTE

from aibs_informatics_aws_lambda.common.handler import LambdaHandler


def get_s3_scratch_key(
    filename: Optional[str] = None,
    content: Optional[JSON] = None,
    unique_id: Optional[UniqueID] = None,
) -> S3Key:
    """Generates a scratch file s3 key

    The key is constructed from filename, content and unique ID.

    If filename is not provided, a hexdigest is created from content (which will
    be random if content is None).

    Args:
        filename (Optional[str], optional): Optional name of file.
            If None, file hash is generated.
        content (Optional[JSON], optional): Optional content of file to put.
            Only used if filename is not provided. Defaults to None.
        unique_id (Optional[UniqueID], optional): A unique ID used in .
            If None, a random UUID is generated.

    Returns:
        S3Key: S3 Scratch key (not gauranteed to be empty)
    """
    file_hash = filename or sha256_hexdigest(content=content)
    return S3Key(f"scratch/{unique_id or UniqueID.create()}/{file_hash}")


DEFAULT_BUCKET_NAME_ENV_VAR = "DEFAULT_BUCKET_NAME"


class GetJSONFromFileHandler(LambdaHandler[GetJSONFromFileRequest, GetJSONFromFileResponse]):
    def handle(self, request: GetJSONFromFileRequest) -> GetJSONFromFileResponse:
        try:
            path = request.path

            self.logger.info(f"Fetching content from {path}")
            if isinstance(path, S3URI):
                self.logger.info(f"Downloading from S3")
                content = download_to_json(s3_path=path)
            else:
                self.logger.info(f"Loading from path")
                content = load_json(path)
            return GetJSONFromFileResponse(content=content)
        except Exception as e:
            self.logger.error(f"Could not fetch content from {request.path}")
            raise e


class PutJSONToFileHandler(LambdaHandler[PutJSONToFileRequest, PutJSONToFileResponse]):
    def handle(self, request: PutJSONToFileRequest) -> Optional[PutJSONToFileResponse]:
        path, content = request.path, request.content

        if path is None:
            bucket_name = get_env_var(DEFAULT_BUCKET_NAME_ENV_VAR, default_value=None)
            if bucket_name is None:
                raise ValueError(
                    "No path provided and Could not infer bucket "
                    f"name from {DEFAULT_BUCKET_NAME_ENV_VAR} environment variable"
                )
            path = S3URI.build(
                bucket_name=bucket_name,
                key=get_s3_scratch_key(
                    content=content,
                    unique_id=UniqueID(self.context.aws_request_id),
                ),
            )

        self.logger.info(f"Writing content to {path}")
        self.logger.info(f"Content to write: {content}")
        if isinstance(path, S3URI):
            self.logger.info(f"Uploading to S3")
            upload_json(content, s3_path=path, extra_args=SCRATCH_EXTRA_ARGS)
        else:
            self.logger.info(f"Writing to file")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(content, indent=4, sort_keys=True))
        return PutJSONToFileResponse(path=path)


class DataSyncHandler(LambdaHandler[DataSyncRequest, DataSyncResponse]):
    def handle(self, request: DataSyncRequest) -> DataSyncResponse:
        sync_operations = DataSyncOperations(request)
        result = sync_operations.sync_task(request)
        return DataSyncResponse(request=request, result=result)


class BatchDataSyncHandler(LambdaHandler[BatchDataSyncRequest, BatchDataSyncResponse]):
    def handle(self, request: BatchDataSyncRequest) -> BatchDataSyncResponse:
        self.logger.info(f"Received {len(request.requests)} requests to transfer")
        if isinstance(request.requests, S3URI):
            self.logger.info(f"Request is stored at {request.requests}... fetching content.")
            _ = download_to_json(request.requests)
            assert isinstance(_, list)
            batch_requests = [DataSyncRequest.from_dict(__) for __ in _]
        else:
            batch_requests = request.requests

        batch_result = BatchDataSyncResult()
        response = BatchDataSyncResponse(result=batch_result, failed_requests=[])

        for i, _ in enumerate(batch_requests):
            sync_operations = DataSyncOperations(_)
            self.logger.info(
                f"[{i+1}/{len(batch_requests)}] "
                f"Syncing content from {_.source_path} to {_.destination_path}"
            )
            try:
                result = sync_operations.sync(
                    source_path=_.source_path,
                    destination_path=_.destination_path,
                    source_path_prefix=_.source_path_prefix,
                )
                if result.bytes_transferred is not None:
                    batch_result.add_bytes_transferred(result.bytes_transferred)
                if result.files_transferred is not None:
                    batch_result.add_files_transferred(result.files_transferred)
                batch_result.increment_successful_requests_count()

                if result.bytes_transferred:
                    result.add_bytes_transferred(result.bytes_transferred)
            except Exception as e:
                batch_result.increment_failed_requests_count()
                response.add_failed_request(_)
                self.logger.error(
                    f"Failed to sync content from {_.source_path} to {_.destination_path}"
                )
                self.logger.error(e)
                if not request.allow_partial_failure:
                    raise e
        return response


class PrepareBatchDataSyncHandler(
    LambdaHandler[PrepareBatchDataSyncRequest, PrepareBatchDataSyncResponse]
):
    DEFAULT_SOFT_MAX_BYTES: int = 250 * BYTES_PER_GIBIBYTE  # 250 GiB

    def handle(self, request: PrepareBatchDataSyncRequest) -> PrepareBatchDataSyncResponse:
        self.logger.info("Preparing S3 Batch Sync Requests")
        root: Union[S3FileSystem, LocalFileSystem]
        if isinstance(request.source_path, S3URI):
            root = S3FileSystem.from_path(request.source_path)
        else:
            root = LocalFileSystem.from_path(request.source_path)
        batch_size_bytes_limit = request.batch_size_bytes_limit or self.DEFAULT_SOFT_MAX_BYTES

        ## We will use a revised version of the bin packing problem:
        # https://en.wikipedia.org/wiki/Bin_packing_problem

        # Step 1A: Partition nodes s.t. we deal with fewer paths in total.
        self.logger.info(f"Partitioning batch size bytes limit: {batch_size_bytes_limit}")
        nodes = root.partition(size_bytes_limit=batch_size_bytes_limit)

        batch_data_sync_requests: List[BatchDataSyncRequest] = []

        node_batches = self.build_node_batches(nodes, batch_size_bytes_limit)
        self.logger.info(f"Batched {len(nodes)} nodes into {len(node_batches)} batches")
        for node_batch in node_batches:
            data_sync_requests: List[DataSyncRequest] = []
            for node in sorted(node_batch):
                data_sync_requests.append(
                    DataSyncRequest(
                        source_path=self.build_source_path(request, node),
                        destination_path=self.build_destination_path(request, node),
                        source_path_prefix=request.source_path_prefix,
                        max_concurrency=request.max_concurrency,
                        retain_source_data=request.retain_source_data,
                        require_lock=request.require_lock,
                    )
                )
            batch_data_sync_requests.append(BatchDataSyncRequest(requests=data_sync_requests))

        if request.temporary_request_payload_path:
            self.logger.info(
                f"Uploading batch requests to {request.temporary_request_payload_path}"
            )
            new_batch_data_sync_requests = []

            for i, batch_data_sync_request in enumerate(batch_data_sync_requests):
                upload_json(
                    [cast(DataSyncRequest, _).to_dict() for _ in batch_data_sync_request.requests],
                    s3_path=(
                        s3_path := request.temporary_request_payload_path / f"request_{i}.json"
                    ),
                )
                new_batch_data_sync_requests.append(BatchDataSyncRequest(requests=s3_path))
            return PrepareBatchDataSyncResponse(requests=new_batch_data_sync_requests)
        else:
            return PrepareBatchDataSyncResponse(requests=batch_data_sync_requests)

    @classmethod
    def build_source_path(
        cls, request: PrepareBatchDataSyncRequest, node: Node
    ) -> Union[S3URI, Path]:
        if isinstance(request.source_path, S3URI):
            return S3URI.build(bucket_name=request.source_path.bucket, key=node.path)
        else:
            return Path(node.path)

    @classmethod
    def build_destination_path(
        cls, request: PrepareBatchDataSyncRequest, node: Node
    ) -> Union[S3URI, Path]:
        source_path = request.source_path
        source_prefix = source_path.key if isinstance(source_path, S3URI) else f"{source_path}"
        relative_path = removeprefix(node.path, prefix=source_prefix).lstrip("/")

        # NOTE: This is because Path instances omit the file separator.
        #       S3 is sensitive to this, so we must append it intentionally.
        #       We should figure out a cleaner solution but this will have to do.
        if node.has_children():
            relative_path += "/"
        if isinstance(request.destination_path, S3URI):
            return S3URI.build(
                bucket_name=request.destination_path.bucket,
                key=request.destination_path.key + relative_path,
            )
        else:
            return Path(f"{request.destination_path}/{relative_path}")

    @classmethod
    def build_node_batches(
        cls, nodes: List[Node], batch_size_bytes_limit: int
    ) -> List[List[Node]]:
        """Batch nodes based on threshold

        This is a version of the classic "Bin Packing" problem.
        https://en.wikipedia.org/wiki/Bin_packing_problem

        The following solutions implements the First-fit decreasing algorithm.

        Notes:
            - nodes can have sizes greater than the limit

        Args:
            nodes (List[Node]): List of batch
            batch_size_bytes_limit (int): size limit in bytes for a batch of nodes

        Returns:
            List[List[Node]]: List of node batches (list of lists)
        """

        ## We will use a revised version of the bin packing problem:
        # https://en.wikipedia.org/wiki/Bin_packing_problem

        # Step 1: and then sort the nodes by size (descending order)
        unbatched_nodes = sorted(nodes, key=lambda node: node.size_bytes, reverse=True)

        # Step 2: Group nodes in order to maximize the data synced per request (bin packing problem)
        node_batches: List[List[Node]] = []

        # (Optimize) Convert all nodes that are larger than the threshold into single requests.
        while unbatched_nodes and unbatched_nodes[0].size_bytes > batch_size_bytes_limit:
            node_batches.append([unbatched_nodes.pop(0)])

        # Compute the batch node sizes (they should all be larger than the)
        node_batch_sizes = [
            sum([node.size_bytes for node in node_batch]) for node_batch in node_batches
        ]

        for node in unbatched_nodes:
            for i in range(len(node_batch_sizes)):
                if node_batch_sizes[i] + node.size_bytes <= batch_size_bytes_limit:
                    node_batch_sizes[i] += node.size_bytes
                    node_batches[i].append(node)
                    break
            else:
                node_batch_sizes.append(node.size_bytes)
                node_batches.append([node])

        return node_batches
