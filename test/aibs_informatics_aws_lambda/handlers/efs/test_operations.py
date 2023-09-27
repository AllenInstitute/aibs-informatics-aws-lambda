from datetime import timedelta
from pathlib import Path
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase
from time import sleep
from typing import Tuple, Union

from aibs_informatics_aws_utils.data_sync.file_system import LocalFileSystem, PathStats
from aibs_informatics_aws_lambda.common.api_handler import LambdaHandlerType
from aibs_informatics_aws_lambda.handlers.efs.model import (
    GetEFSPathStatsRequest,
    GetEFSPathStatsResponse,
    OutdatedEFSPathScannerRequest,
    OutdatedEFSPathScannerResponse,
    RemoveEFSPathsRequest,
    RemoveEFSPathsResponse,
)
from aibs_informatics_aws_lambda.handlers.efs.operations import (
    EFSHandlerMixins,
    GetEFSPathStatsHandler,
    OutdatedEFSPathScannerHandler,
    RemoveEFSPathsHandler,
)


class EFSOperationHandlerTestCase(LambdaHandlerTestCase):
    def add_files_to_file_system(self, root: Path, *paths: Tuple[Union[Path, str], int]) -> Path:
        full_paths = [(root / p, sz) for p, sz in paths]

        for path, sz in full_paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("a" * sz)

        return root


class GetEFSPathStatsHandlerTests(EFSOperationHandlerTestCase):
    @property
    def handler(self) -> LambdaHandlerType:
        return GetEFSPathStatsHandler.get_handler()

    def test__handles__simple__fetches_root_stats(self):
        root = self.tmp_path()
        self.add_files_to_file_system(
            root,
            ("x", 1),
            ("y", 1),
        )
        efs_root = EFSHandlerMixins.get_efs_file_system_root("/", root)

        request = GetEFSPathStatsRequest(
            path=Path("/"),
            efs_mount_path=root,
        )
        response = GetEFSPathStatsResponse(
            efs_mount_path=root,
            path=Path("."),
            path_stats=PathStats(
                last_modified=efs_root.node.last_modified,
                size_bytes=2,
                object_count=2,
            ),
            children={
                "x": PathStats(
                    last_modified=efs_root.node.children["x"].last_modified,
                    size_bytes=1,
                    object_count=1,
                ),
                "y": PathStats(
                    last_modified=efs_root.node.children["y"].last_modified,
                    size_bytes=1,
                    object_count=1,
                ),
            },
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())


class OutdatedEFSPathScannerHandlerTests(EFSOperationHandlerTestCase):
    @property
    def handler(self) -> LambdaHandlerType:
        return OutdatedEFSPathScannerHandler.get_handler()

    def test__handles__simple__all_outdated(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        request = OutdatedEFSPathScannerRequest(
            efs_mount_path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_efs_size_bytes=0,
        )
        response = OutdatedEFSPathScannerResponse(paths=[Path(".")], efs_mount_path=root)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__simple__partial_outdated(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        sleep(0.01)
        self.add_files_to_file_system(root, ("c", 1), ("d", 1), ("e/x", 1))
        request = OutdatedEFSPathScannerRequest(
            efs_mount_path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_efs_size_bytes=0,
        )
        response = OutdatedEFSPathScannerResponse(
            paths=[Path("a"), Path("b")], efs_mount_path=root
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__all_outdated__min_depth_specified(self):
        root = self.tmp_path()
        self.add_files_to_file_system(
            root, ("x/a", 1), ("x/b", 1), ("x/c/1", 1), ("x/c/2", 1), ("y", 1)
        )
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        request = OutdatedEFSPathScannerRequest(
            efs_mount_path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_depth=2,
            min_efs_size_bytes=0,
        )
        response = OutdatedEFSPathScannerResponse(
            paths=[Path("x/a"), Path("x/b"), Path("x/c"), Path("y")], efs_mount_path=root
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__partial_outdated__sub_section(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a/old", 1), ("b/old", 1))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        sleep(0.01)
        self.add_files_to_file_system(root, ("a/new", 1), ("b/new", 1))
        request = OutdatedEFSPathScannerRequest(
            efs_mount_path=root,
            path=Path("b"),
            days_since_last_accessed=0,
            current_time=current_time,
            min_efs_size_bytes=0,
        )
        response = OutdatedEFSPathScannerResponse(paths=[Path("b/old")], efs_mount_path=root)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__partial_outdated__sub_section__max_depth_specified(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a/1/old", 1), ("b/b2/b3/old", 1))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        sleep(0.01)
        self.add_files_to_file_system(root, ("a/1/new", 1), ("b/b2/b3/new", 1))
        request = OutdatedEFSPathScannerRequest(
            efs_mount_path=root,
            path=Path("b"),
            days_since_last_accessed=0,
            current_time=current_time,
            max_depth=2,
            min_efs_size_bytes=0,
        )
        response = OutdatedEFSPathScannerResponse(paths=[], efs_mount_path=root)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        request.max_depth = 3
        response = OutdatedEFSPathScannerResponse(paths=["b/b2/b3/old"], efs_mount_path=root)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__min_efs_size_bytes__simple_case(self):
        # File 'a' would be removed but due to `min_efs_size_bytes=20` it should be retained.
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 5))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        sleep(0.01)
        self.add_files_to_file_system(root, ("c", 5), ("d", 5), ("e", 5))
        request = OutdatedEFSPathScannerRequest(
            efs_mount_path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_efs_size_bytes=20,
        )
        response = OutdatedEFSPathScannerResponse(paths=[], efs_mount_path=root)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())


class RemoveEFSPathsHandlerTests(EFSOperationHandlerTestCase):
    @property
    def handler(self) -> LambdaHandlerType:
        return RemoveEFSPathsHandler.get_handler()

    def test__handles__simple(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1))
        request = RemoveEFSPathsRequest(
            paths=[Path("/")],
            efs_mount_path=root,
        )
        response = RemoveEFSPathsResponse(size_bytes_removed=2)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertFalse((root / "a").exists())
        self.assertFalse((root / "b").exists())

    def test__handles__partial_removal(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("scratch/a", 1), ("scratch/b", 1))
        request = RemoveEFSPathsRequest(
            paths=[Path("scratch")],
            efs_mount_path=root,
        )
        response = RemoveEFSPathsResponse(size_bytes_removed=2)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertFalse((root / "scratch" / "a").exists())
        self.assertFalse((root / "scratch" / "b").exists())

    def test__handles__partial_removal__sub_dir(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1), ("c/1", 1), ("c/2", 1))
        request = RemoveEFSPathsRequest(
            paths=[Path("b"), Path("c")],
            efs_mount_path=root,
        )
        response = RemoveEFSPathsResponse(size_bytes_removed=3)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertTrue((root / "a").exists())
        self.assertFalse((root / "b").exists())
        self.assertFalse((root / "c/1").exists())
        self.assertFalse((root / "c/2").exists())

    def test__handles__partial_removal__duplicate(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1), ("c/1", 1), ("c/2", 1))
        request = RemoveEFSPathsRequest(
            paths=[Path("b"), Path("c"), Path("c")],
            efs_mount_path=root,
        )
        response = RemoveEFSPathsResponse(size_bytes_removed=3)
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertTrue((root / "a").exists())
        self.assertFalse((root / "b").exists())
        self.assertFalse((root / "c/1").exists())
        self.assertFalse((root / "c/2").exists())
