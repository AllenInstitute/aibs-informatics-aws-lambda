"""Data synchronization models.

Defines the request and response models for data sync operations
between S3, EFS, and local file systems.
"""

import re
from datetime import datetime
from functools import cached_property
from pathlib import Path
from re import Pattern
from typing import TypeAlias

from aibs_informatics_aws_utils.data_sync.file_system import PathStats
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.base import (
    PydanticBaseModel,
)
from aibs_informatics_core.utils.time import get_current_time
from pydantic import Field

# Order matters for this union - S3Path and EFSPath have custom validation logic that would
# allow them to be parsed from a string
DataPath: TypeAlias = S3Path | EFSPath | Path | str


class WithDataPath(PydanticBaseModel):
    """Base class for models that contain a data path.

    Provides convenience properties for accessing the path as different types.

    Attributes:
        path: The data path (S3, EFS, or local).
    """

    path: DataPath

    @property
    def efs_path(self) -> EFSPath | None:
        """Get the path as an EFS path if applicable.

        Returns:
            The EFS path or None if not an EFS path.
        """
        if isinstance(self.path, EFSPath):
            return self.path
        return None

    @property
    def s3_uri(self) -> S3Path | None:
        """Get the path as an S3 URI if applicable.

        Returns:
            The S3 path or None if not an S3 path.
        """
        if isinstance(self.path, S3Path):
            return self.path
        return None

    @property
    def local_path(self) -> Path | None:
        """Get the path as a local path if applicable.

        Returns:
            The local path or None if not a local path.
        """
        if isinstance(self.path, Path):
            return self.path
        return None


class ListDataPathsRequest(WithDataPath):
    """Request for listing files under a data path.

    Supports filtering with include/exclude regex patterns.

    Attributes:
        path: The data path under which to list files.
        include: Optional regex pattern(s) for files to include.
            If multiple patterns, includes files matching any pattern.
        exclude: Optional regex pattern(s) for files to exclude.
            Exclude patterns take precedence over include patterns.
    """

    include: str | list[str] | None = None
    exclude: str | list[str] | None = None

    @cached_property
    def include_patterns(self) -> list[Pattern] | None:
        return self._get_patterns(self.include)

    @cached_property
    def exclude_patterns(self) -> list[Pattern] | None:
        return self._get_patterns(self.exclude)

    @staticmethod
    def _get_patterns(value: str | list[str] | None) -> list[Pattern] | None:
        if not value:
            return None
        return [re.compile(p) for p in ([value] if isinstance(value, str) else value)]


class ListDataPathsResponse(PydanticBaseModel):
    """Response containing listed data paths.

    Attributes:
        paths: List of data paths found.
    """

    paths: list[DataPath] = Field(default_factory=list)


class RemoveDataPathsRequest(PydanticBaseModel):
    """Request for removing data paths.

    Attributes:
        paths: List of data paths to remove.
    """

    paths: list[DataPath] = Field(default_factory=list)


class RemoveDataPathsResponse(PydanticBaseModel):
    """Response from removing data paths.

    Attributes:
        size_bytes_removed: Total bytes removed.
        paths_removed: List of paths that were removed.
    """

    size_bytes_removed: int
    paths_removed: list[DataPath] = Field(default_factory=list)


class OutdatedDataPathScannerRequest(WithDataPath):
    """Request for scanning outdated data paths.

    Scans for paths that haven't been accessed within a specified time.

    Attributes:
        path: The root path to scan.
        days_since_last_accessed: Minimum days since last access to be outdated.
        max_depth: Maximum directory depth to scan.
        min_depth: Minimum directory depth to scan.
        min_size_bytes_allowed: Minimum size threshold for paths to include.
        current_time: Reference time for calculating age.
    """

    days_since_last_accessed: float = 0.0
    max_depth: int | None = None
    min_depth: int | None = None
    min_size_bytes_allowed: int = 0
    current_time: datetime = Field(default_factory=get_current_time)


class OutdatedDataPathScannerResponse(PydanticBaseModel):
    """Response containing outdated data paths.

    Attributes:
        paths: List of paths identified as outdated.
    """

    paths: list[DataPath] = Field(default_factory=list)


class GetDataPathStatsRequest(WithDataPath):
    """Request for getting statistics about a data path.

    Attributes:
        path: The data path to get statistics for.
    """

    pass


class GetDataPathStatsResponse(WithDataPath):
    """Response containing data path statistics.

    Attributes:
        path: The data path.
        path_stats: Statistics for the path.
        children: Statistics for child paths keyed by name.
    """

    path_stats: PathStats
    children: dict[str, PathStats] = Field(default_factory=dict)
