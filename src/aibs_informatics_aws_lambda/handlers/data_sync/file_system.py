import logging
from datetime import timedelta
from pathlib import Path
from typing import List, TypeVar
from xml.etree.ElementInclude import include

from aibs_informatics_aws_utils.data_sync.file_system import BaseFileSystem, Node, get_file_system
from aibs_informatics_aws_utils.efs import detect_mount_points, get_local_path
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.utils.file_operations import (
    get_path_size_bytes,
    remove_path,
    strip_path_root,
)

from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_aws_lambda.handlers.data_sync.model import (
    DataPath,
    GetDataPathStatsRequest,
    GetDataPathStatsResponse,
    ListDataPathsRequest,
    ListDataPathsResponse,
    OutdatedDataPathScannerRequest,
    OutdatedDataPathScannerResponse,
    RemoveDataPathsRequest,
    RemoveDataPathsResponse,
)

logger = logging.getLogger(__name__)

FS = TypeVar("FS", bound=BaseFileSystem)


class GetDataPathStatsHandler(LambdaHandler[GetDataPathStatsRequest, GetDataPathStatsResponse]):
    def handle(self, request: GetDataPathStatsRequest) -> GetDataPathStatsResponse:
        root = get_file_system(request.path)
        node = root.node
        return GetDataPathStatsResponse(
            path=node.path,
            path_stats=node.path_stats,
            children={
                child_path: child_node.path_stats
                for child_path, child_node in node.children.items()
            },
        )


class ListDataPathsHandler(LambdaHandler[ListDataPathsRequest, ListDataPathsResponse]):
    def handle(self, request: ListDataPathsRequest) -> ListDataPathsResponse:
        root = get_file_system(request.path)
        paths: List[DataPath] = sorted([n.path for n in root.node.list_nodes()])

        if request.include_patterns or request.exclude_patterns:
            new_paths = []
            for path in paths:
                rel_path = strip_path_root(path, root.node.path)
                if request.include_patterns:
                    if not any([i.match(rel_path) for i in request.include_patterns]):
                        continue
                if request.exclude_patterns:
                    if any([i.match(rel_path) for i in request.exclude_patterns]):
                        continue
                new_paths.append(path)
            paths = new_paths
        return ListDataPathsResponse(paths=paths)


class OutdatedDataPathScannerHandler(
    LambdaHandler[OutdatedDataPathScannerRequest, OutdatedDataPathScannerResponse],
):
    def handle(self, request: OutdatedDataPathScannerRequest) -> OutdatedDataPathScannerResponse:
        """Determine paths to delete from EFS in a 2 step process

        1) Determine stale file nodes whose days_since_last_accessed exceed our minimum
        2) Sort stale nodes such that oldest are considered first and assess whether deletion of
           the files represented by the node would make our total EFS size too small
        """
        fs = get_file_system(request.path)

        stale_nodes: List[Node] = []
        days_since_last_accessed = timedelta(days=request.days_since_last_accessed)
        unvisited_nodes: List[Node] = [fs.node]

        self.logger.info(
            f"Checking for nodes older than {request.days_since_last_accessed} days. "
            f"Max depth = {request.max_depth}"
        )
        # Step 1)
        while unvisited_nodes:
            node = unvisited_nodes.pop()
            if (request.current_time - node.last_modified) > days_since_last_accessed:
                if (
                    request.min_depth is None
                    or (node.depth - fs.node.depth) >= request.min_depth
                    or not node.has_children()
                ):
                    stale_nodes.append(node)
                else:
                    unvisited_nodes.extend(node.children.values())
            elif request.max_depth is None or (node.depth - fs.node.depth) < request.max_depth:
                unvisited_nodes.extend(node.children.values())

        # Step 2)
        # Get the current size of the EFS volume, this is used to ensure we do not delete too
        # many files and allows us to maintain a minimum desired EFS throughput performance.
        # For more details see: https://docs.aws.amazon.com/efs/latest/ug/performance.html
        current_efs_size_bytes = fs.node.size_bytes
        paths_to_delete: List[str] = []

        # Sort so newest nodes are first, nodes are considered starting from the list end (oldest)
        nodes_to_delete = sorted(stale_nodes, key=lambda n: n.last_modified, reverse=True)
        while nodes_to_delete and current_efs_size_bytes > request.min_size_bytes_allowed:
            node = nodes_to_delete.pop()
            paths_to_delete.append(node.path)
            current_efs_size_bytes -= node.size_bytes

        return OutdatedDataPathScannerResponse(
            paths=sorted(paths_to_delete),
        )


class RemoveDataPathsHandler(LambdaHandler[RemoveDataPathsRequest, RemoveDataPathsResponse]):
    def handle(self, request: RemoveDataPathsRequest) -> RemoveDataPathsResponse:
        self.logger.info(f"Removing {len(request.paths)}")

        mount_points = None
        size_bytes_removed = 0
        paths_removed = []
        for path in request.paths:
            if isinstance(path, S3URI):
                # # TODO: add support for S3URI when more guardrails are in place
                # path_stats = get_s3_path_stats(path)
                # delete_s3_path(path)
                # size_bytes_removed += path_stats.size_bytes
                self.logger.warning(f"Skipping S3URI path deletion ({path}). Not supported yet.")
            else:
                if isinstance(path, EFSPath):
                    self.logger.info(f"Converting EFSPath ({path}) to local path")
                    if mount_points is None:
                        mount_points = detect_mount_points()
                    path = get_local_path(efs_path=path, mount_points=mount_points)
                elif not isinstance(path, Path):
                    path = Path(path)
                try:
                    size_bytes = get_path_size_bytes(path)
                    self.logger.info(f"Removing {path} (size {size_bytes} bytes)")
                    remove_path(path)
                    size_bytes_removed += size_bytes
                    paths_removed.append(path)
                except FileNotFoundError as e:
                    self.logger.warning(f"File at {path} does not exist anymore. Reason: {e}")
        return RemoveDataPathsResponse(size_bytes_removed, paths_removed)
