from datetime import timedelta
from pathlib import Path
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase
from time import sleep
from typing import Tuple, Union

from aibs_informatics_aws_utils.data_sync.file_system import LocalFileSystem, PathStats

from aibs_informatics_aws_lambda.common.handler import LambdaHandlerType
from aibs_informatics_aws_lambda.handlers.data_sync.file_system import (
    GetDataPathStatsHandler,
    ListDataPathsHandler,
    OutdatedDataPathScannerHandler,
    RemoveDataPathsHandler,
)
from aibs_informatics_aws_lambda.handlers.data_sync.model import (
    GetDataPathStatsRequest,
    GetDataPathStatsResponse,
    ListDataPathsRequest,
    ListDataPathsResponse,
    OutdatedDataPathScannerRequest,
    OutdatedDataPathScannerResponse,
    RemoveDataPathsRequest,
    RemoveDataPathsResponse,
)


class BaseFileSystemHandlerTestCase(LambdaHandlerTestCase):
    def add_files_to_file_system(self, root: Path, *paths: Tuple[Union[Path, str], int]) -> Path:
        full_paths = [(root / p, sz) for p, sz in paths]

        for path, sz in full_paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("a" * sz)

        return root


class ListDataPathsHandlerTests(BaseFileSystemHandlerTestCase):
    @property
    def handler(self) -> LambdaHandlerType:
        return ListDataPathsHandler.get_handler()

    def test__handles__simple__fetches_all_paths(self):
        root = self.tmp_path()
        self.add_files_to_file_system(
            root,
            ("x", 1),
            ("y", 1),
        )

        request = ListDataPathsRequest(path=root)
        response = ListDataPathsResponse(paths=[f"{root}/", root / "x", root / "y"])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__nested_paths(self):
        root = self.tmp_path()
        self.add_files_to_file_system(
            root,
            ("x1/y1/z1", 1),
            ("x1/y1/z2", 1),
            ("x1/y2/z1", 1),
            ("x1/y2/z2", 1),
            ("x2/y1/z1", 1),
            ("x2/y2/z2", 1),
        )

        request = ListDataPathsRequest(path=root / "x1")
        response = ListDataPathsResponse(
            paths=[
                f"{root}/x1/",
                f"{root}/x1/y1/",
                f"{root}/x1/y1/z1",
                f"{root}/x1/y1/z2",
                f"{root}/x1/y2/",
                f"{root}/x1/y2/z1",
                f"{root}/x1/y2/z2",
            ]
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__include_regex_patterns(self):
        root = self.prep_regex_pattern_test()

        # Single include pattern
        self.assertHandles(
            self.handler,
            ListDataPathsRequest(path=root, include=".*A.*").to_dict(),
            ListDataPathsResponse(
                paths=[
                    f"{root}/A/",
                    f"{root}/A/B/",
                    f"{root}/A/B/C",
                    f"{root}/A/B/D",
                    f"{root}/A/C/",
                    f"{root}/A/C/E",
                    f"{root}/A/C/F",
                    f"{root}/C/B/A",
                ]
            ).to_dict(),
        )

        # Multiple include patterns
        self.assertHandles(
            self.handler,
            ListDataPathsRequest(path=root, include=[f"A.*", f".*B.*"]).to_dict(),
            ListDataPathsResponse(
                paths=[
                    f"{root}/A/",
                    f"{root}/A/B/",
                    f"{root}/A/B/C",
                    f"{root}/A/B/D",
                    f"{root}/A/C/",
                    f"{root}/A/C/E",
                    f"{root}/A/C/F",
                    f"{root}/B/",
                    f"{root}/B/C/",
                    f"{root}/B/C/G",
                    f"{root}/B/D/",
                    f"{root}/B/D/H",
                    f"{root}/C/B/",
                    f"{root}/C/B/A",
                ]
            ).to_dict(),
        )

    def test__handles__regex_exclude_patterns(self):
        root = self.prep_regex_pattern_test()

        # single exclude pattern
        self.assertHandles(
            self.handler,
            ListDataPathsRequest(path=root, exclude=".*B.*").to_dict(),
            ListDataPathsResponse(
                paths=[
                    f"{root}/",
                    f"{root}/A/",
                    f"{root}/A/C/",
                    f"{root}/A/C/E",
                    f"{root}/A/C/F",
                    f"{root}/C/",
                ]
            ).to_dict(),
        )

        # multiple exclude patterns
        self.assertHandles(
            self.handler,
            ListDataPathsRequest(path=root, exclude=[".*A.*", ".*B.*"]).to_dict(),
            ListDataPathsResponse(
                paths=[
                    f"{root}/",
                    f"{root}/C/",
                ]
            ).to_dict(),
        )

    def test__handles__include_exclude_patterns(self):
        root = self.prep_regex_pattern_test()

        # include and exclude patterns
        self.assertHandles(
            self.handler,
            ListDataPathsRequest(path=root, include=".*A.*", exclude=".*B.*").to_dict(),
            ListDataPathsResponse(
                paths=[
                    f"{root}/A/",
                    f"{root}/A/C/",
                    f"{root}/A/C/E",
                    f"{root}/A/C/F",
                ]
            ).to_dict(),
        )

    def prep_regex_pattern_test(self):
        root = self.tmp_path()
        self.add_files_to_file_system(
            root,
            ("A/B/C", 1),
            ("A/B/D", 1),
            ("A/C/E", 1),
            ("A/C/F", 1),
            ("B/C/G", 1),
            ("B/D/H", 1),
            ("C/B/A", 1),
        )
        return root


class GetDataPathStatsHandlerTests(BaseFileSystemHandlerTestCase):
    @property
    def handler(self) -> LambdaHandlerType:
        return GetDataPathStatsHandler.get_handler()

    def test__handles__simple__fetches_root_stats(self):
        root = self.tmp_path()
        self.add_files_to_file_system(
            root,
            ("x", 1),
            ("y", 1),
        )
        file_system = LocalFileSystem.from_path(root)

        request = GetDataPathStatsRequest(path=root)
        response = GetDataPathStatsResponse(
            path=f"{root.as_posix()}/",
            path_stats=PathStats(
                last_modified=file_system.node.last_modified,
                size_bytes=2,
                object_count=2,
            ),
            children={
                "x": PathStats(
                    last_modified=file_system.node.children["x"].last_modified,
                    size_bytes=1,
                    object_count=1,
                ),
                "y": PathStats(
                    last_modified=file_system.node.children["y"].last_modified,
                    size_bytes=1,
                    object_count=1,
                ),
            },
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())


class OutdatedDataPathScannerHandlerTests(BaseFileSystemHandlerTestCase):
    @property
    def handler(self) -> LambdaHandlerType:
        return OutdatedDataPathScannerHandler.get_handler()

    def test__handles__simple__all_outdated(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        request = OutdatedDataPathScannerRequest(
            path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_size_bytes_allowed=0,
        )
        response = OutdatedDataPathScannerResponse(paths=[root.as_posix() + "/"])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__simple__partial_outdated(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        sleep(0.01)
        self.add_files_to_file_system(root, ("c", 1), ("d", 1), ("e/x", 1))
        request = OutdatedDataPathScannerRequest(
            path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_size_bytes_allowed=0,
        )
        response = OutdatedDataPathScannerResponse(paths=[root / "a", root / "b"])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__all_outdated__min_depth_specified(self):
        root = self.tmp_path()
        self.add_files_to_file_system(
            root, ("x/a", 1), ("x/b", 1), ("x/c/1", 1), ("x/c/2", 1), ("y", 1)
        )
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        request = OutdatedDataPathScannerRequest(
            path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_depth=2,
            min_size_bytes_allowed=0,
        )
        response = OutdatedDataPathScannerResponse(
            paths=[root / "x/a", root / "x/b", (root / "x/c").as_posix() + "/", root / "y"]
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
        request = OutdatedDataPathScannerRequest(
            path=root / "b",
            days_since_last_accessed=0,
            current_time=current_time,
            min_size_bytes_allowed=0,
        )
        response = OutdatedDataPathScannerResponse(paths=[root / "b/old"])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__partial_outdated__sub_section__max_depth_specified(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a/1/old", 1), ("b/b2/b3/old", 1))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        sleep(0.01)
        self.add_files_to_file_system(root, ("a/1/new", 1), ("b/b2/b3/new", 1))
        request = OutdatedDataPathScannerRequest(
            path=root / "b",
            days_since_last_accessed=0,
            current_time=current_time,
            max_depth=2,
            min_size_bytes_allowed=0,
        )
        response = OutdatedDataPathScannerResponse(paths=[])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        request.max_depth = 3
        response = OutdatedDataPathScannerResponse(paths=[root / "b/b2/b3/old"])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

    def test__handles__min_size_bytes_allowed__simple_case(self):
        # File 'a' would be removed but due to `min_size_bytes_allowed=20` it should be retained.
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 5))
        current_time = LocalFileSystem.from_path(root).node.last_modified + timedelta(
            microseconds=1.0
        )
        sleep(0.01)
        self.add_files_to_file_system(root, ("c", 5), ("d", 5), ("e", 5))
        request = OutdatedDataPathScannerRequest(
            path=root,
            days_since_last_accessed=0,
            current_time=current_time,
            min_size_bytes_allowed=20,
        )
        response = OutdatedDataPathScannerResponse(paths=[])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())


class RemoveDataPathsHandlerTests(BaseFileSystemHandlerTestCase):
    @property
    def handler(self) -> LambdaHandlerType:
        return RemoveDataPathsHandler.get_handler()

    def test__handles__simple(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1))
        request = RemoveDataPathsRequest(paths=[root])
        response = RemoveDataPathsResponse(size_bytes_removed=2, paths_removed=[root.as_posix()])
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertFalse((root / "a").exists())
        self.assertFalse((root / "b").exists())

    def test__handles__partial_removal(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("scratch/a", 1), ("scratch/b", 1), ("c", 1))
        request = RemoveDataPathsRequest(paths=[root / "scratch"])
        response = RemoveDataPathsResponse(
            size_bytes_removed=2, paths_removed=[(root / "scratch").as_posix()]
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertFalse((root / "scratch" / "a").exists())
        self.assertFalse((root / "scratch" / "b").exists())
        self.assertTrue((root / "c").exists())

    def test__handles__partial_removal__sub_dir(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1), ("c/1", 1), ("c/2", 1))
        request = RemoveDataPathsRequest(paths=[root / "b", root / "c"])
        response = RemoveDataPathsResponse(
            size_bytes_removed=3,
            paths_removed=[(root / "b").as_posix(), (root / "c").as_posix()],
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertTrue((root / "a").exists())
        self.assertFalse((root / "b").exists())
        self.assertFalse((root / "c/1").exists())
        self.assertFalse((root / "c/2").exists())

    def test__handles__partial_removal__duplicate(self):
        root = self.tmp_path()
        self.add_files_to_file_system(root, ("a", 1), ("b", 1), ("c/1", 1), ("c/2", 1))
        request = RemoveDataPathsRequest(paths=[root / "b", root / "c", root / "c"])
        response = RemoveDataPathsResponse(
            size_bytes_removed=3,
            paths_removed=[
                (root / "b").as_posix(),
                (root / "c").as_posix(),
                (root / "c").as_posix(),
            ],
        )
        self.assertHandles(self.handler, request.to_dict(), response=response.to_dict())

        self.assertTrue((root / "a").exists())
        self.assertFalse((root / "b").exists())
        self.assertFalse((root / "c/1").exists())
        self.assertFalse((root / "c/2").exists())
