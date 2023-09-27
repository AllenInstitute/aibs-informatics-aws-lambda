from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Union

from aibs_informatics_aws_utils.data_sync.file_system import LocalFileSystem, Node
from aibs_informatics_aws_utils.efs import get_efs_mount_path
from aibs_informatics_core.utils.file_operations import get_path_size_bytes, remove_path

from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_aws_lambda.handlers.efs.model import (
    GetEFSPathStatsRequest,
    GetEFSPathStatsResponse,
    ListEFSPathsRequest,
    ListEFSPathsResponse,
    OutdatedEFSPathScannerRequest,
    OutdatedEFSPathScannerResponse,
    RemoveEFSPathsRequest,
    RemoveEFSPathsResponse,
)


class EFSHandlerMixins:
    @classmethod
    def resolve_efs_mount_path(cls, efs_mount_path: Optional[Union[str, Path]]) -> Path:
        return Path(get_efs_mount_path(efs_mount_path if efs_mount_path else None))

    @classmethod
    def sanitize_efs_path(cls, path: Union[Path, str], efs_mount_path: Path) -> Path:
        return Path(f"{efs_mount_path}/{path}").resolve()

    @classmethod
    def get_efs_file_system_root(
        cls, path: Optional[Union[str, Path]], efs_mount_path: Optional[Union[str, Path]]
    ) -> LocalFileSystem:
        efs_mount_path = cls.resolve_efs_mount_path(efs_mount_path)

        efs_relative_path = path or Path(".")

        return LocalFileSystem.from_path(cls.sanitize_efs_path(efs_relative_path, efs_mount_path))


class GetEFSPathStatsHandler(
    LambdaHandler[GetEFSPathStatsRequest, GetEFSPathStatsResponse], EFSHandlerMixins
):
    def handle(self, request: GetEFSPathStatsRequest) -> GetEFSPathStatsResponse:
        efs_mount_path = self.resolve_efs_mount_path(request.efs_mount_path)
        efs_root = self.get_efs_file_system_root(path=request.path, efs_mount_path=efs_mount_path)

        node = efs_root.node
        return GetEFSPathStatsResponse(
            efs_mount_path=efs_mount_path,
            path=Path(node.path).relative_to(efs_mount_path),
            path_stats=node.path_stats,
            children={
                child_path: child_node.path_stats
                for child_path, child_node in node.children.items()
            },
        )


class ListEFSPathsHandler(
    LambdaHandler[ListEFSPathsRequest, ListEFSPathsResponse], EFSHandlerMixins
):
    def handle(self, request: ListEFSPathsRequest) -> ListEFSPathsResponse:
        efs_mount_path = self.resolve_efs_mount_path(request.efs_mount_path)
        efs_root = self.get_efs_file_system_root(path=request.path, efs_mount_path=efs_mount_path)

        return ListEFSPathsResponse(
            efs_mount_path=efs_mount_path,
            paths=sorted([Path(n.path) for n in efs_root.node.list_nodes()]),
        )


class OutdatedEFSPathScannerHandler(
    LambdaHandler[OutdatedEFSPathScannerRequest, OutdatedEFSPathScannerResponse],
    EFSHandlerMixins,
):
    def handle(
        self, request: OutdatedEFSPathScannerRequest
    ) -> Optional[OutdatedEFSPathScannerResponse]:
        """Determine paths to delete from EFS in a 2 step process

        1) Determine stale file nodes whose days_since_last_accessed exceed our minimum
        2) Sort stale nodes such that oldest are considered first and assess whether deletion of
           the files represented by the node would make our total EFS size too small
        """
        efs_mount_path = self.resolve_efs_mount_path(request.efs_mount_path)
        fs = self.get_efs_file_system_root(path=request.path, efs_mount_path=efs_mount_path)

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
        paths_to_delete: List[Path] = []

        # Sort so newest nodes are first, nodes are considered starting from the list end (oldest)
        nodes_to_delete = sorted(stale_nodes, key=lambda n: n.last_modified, reverse=True)
        while nodes_to_delete and current_efs_size_bytes > request.min_efs_size_bytes:
            node = nodes_to_delete.pop()
            paths_to_delete.append(Path(node.path).relative_to(efs_mount_path))
            current_efs_size_bytes -= node.size_bytes

        return OutdatedEFSPathScannerResponse(
            paths=sorted(paths_to_delete),
            efs_mount_path=efs_mount_path,
        )


class RemoveEFSPathsHandler(
    LambdaHandler[RemoveEFSPathsRequest, RemoveEFSPathsResponse], EFSHandlerMixins
):
    def handle(self, request: RemoveEFSPathsRequest) -> RemoveEFSPathsResponse:
        self.logger.info(f"Removing {len(request.paths)}")
        efs_mount_path = Path(get_efs_mount_path(str(request.efs_mount_path)))
        self.logger.info(f"Using EFS Root {efs_mount_path}")

        full_paths = self.sanitize_efs_paths(request.paths, efs_mount_path)
        self.logger.info(f"{len(full_paths)} paths requested for removal")

        size_bytes_removed = 0
        for path in full_paths:
            try:
                size_bytes = get_path_size_bytes(path)
                size_bytes_removed += size_bytes
                self.logger.info(f"Removing {path} (size {size_bytes} bytes)")
                remove_path(path)
            except FileNotFoundError as e:
                self.logger.warning(f"File at {path} does not exist anymore. Reason: {e}")
        return RemoveEFSPathsResponse(size_bytes_removed)

    def sanitize_efs_paths(self, paths: List[Path], efs_mount_path: Path) -> List[Path]:
        full_paths = [self.sanitize_efs_path(path, efs_mount_path) for path in paths]

        invalid_paths = [p for p in full_paths if not p.exists()]
        if invalid_paths:
            self.logger.error(
                f"{len(invalid_paths)}/{len(full_paths)} of the paths requested for deletion do not exist!"
            )
            invalid_paths_str = "\n".join(map(str, invalid_paths))
            self.logger.error(f"Following paths do not exist: \n{invalid_paths_str}")

        return full_paths


get_efs_path_stats_handler = GetEFSPathStatsHandler.get_handler()
list_efs_paths_handler = ListEFSPathsHandler.get_handler()
outdated_efs_path_scanner_handler = OutdatedEFSPathScannerHandler.get_handler()
remove_efs_paths_handler = RemoveEFSPathsHandler.get_handler()
