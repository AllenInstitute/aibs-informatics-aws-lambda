"""Data synchronization models.

Defines the request and response models for data sync operations
between S3, EFS, and local file systems.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from re import Pattern
from typing import Dict, List, Optional, Union

from aibs_informatics_aws_utils.data_sync.file_system import PathStats
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.base import (
    CustomAwareDateTime,
    DictField,
    FloatField,
    IntegerField,
    ListField,
    PathField,
    SchemaModel,
    StringField,
    UnionField,
    custom_field,
)
from aibs_informatics_core.utils.time import get_current_time

DataPath = Union[S3Path, EFSPath, Path, str]


def DataPathField(*args, **kwargs):
    return UnionField(
        [
            (S3Path, S3Path.as_mm_field()),
            ((EFSPath, str), EFSPath.as_mm_field()),
            ((Path, str), PathField()),
        ],
        *args,
        **kwargs,
    )


@dataclass
class WithDataPath(SchemaModel):
    """Base class for models that contain a data path.

    Provides convenience properties for accessing the path as different types.

    Attributes:
        path: The data path (S3, EFS, or local).
    """

    path: DataPath = custom_field(mm_field=DataPathField())

    @property
    def efs_path(self) -> Optional[EFSPath]:
        """Get the path as an EFS path if applicable.

        Returns:
            The EFS path or None if not an EFS path.
        """
        if isinstance(self.path, EFSPath):
            return self.path
        return None

    @property
    def s3_uri(self) -> Optional[S3Path]:
        """Get the path as an S3 URI if applicable.

        Returns:
            The S3 path or None if not an S3 path.
        """
        if isinstance(self.path, S3Path):
            return self.path
        return None

    @property
    def local_path(self) -> Optional[Path]:
        """Get the path as a local path if applicable.

        Returns:
            The local path or None if not a local path.
        """
        if isinstance(self.path, Path):
            return self.path
        return None


@dataclass
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

    include: Optional[Union[str, List[str]]] = custom_field(
        default=None,
        mm_field=UnionField([(str, StringField()), (list, ListField(StringField()))]),
    )
    exclude: Optional[Union[str, List[str]]] = custom_field(
        default=None,
        mm_field=UnionField([(str, StringField()), (list, ListField(StringField()))]),
    )

    @cached_property
    def include_patterns(self) -> Optional[List[Pattern]]:
        return self._get_patterns(self.include)

    @cached_property
    def exclude_patterns(self) -> Optional[List[Pattern]]:
        return self._get_patterns(self.exclude)

    @staticmethod
    def _get_patterns(value: Optional[Union[str, List[str]]]) -> Optional[List[Pattern]]:
        if not value:
            return None
        return [re.compile(p) for p in ([value] if isinstance(value, str) else value)]


@dataclass
class ListDataPathsResponse(SchemaModel):
    """Response containing listed data paths.

    Attributes:
        paths: List of data paths found.
    """

    paths: List[DataPath] = custom_field(default_factory=list, mm_field=ListField(DataPathField()))


@dataclass
class RemoveDataPathsRequest(SchemaModel):
    """Request for removing data paths.

    Attributes:
        paths: List of data paths to remove.
    """

    paths: List[DataPath] = custom_field(default_factory=list, mm_field=ListField(DataPathField()))


@dataclass
class RemoveDataPathsResponse(SchemaModel):
    """Response from removing data paths.

    Attributes:
        size_bytes_removed: Total bytes removed.
        paths_removed: List of paths that were removed.
    """

    size_bytes_removed: int = custom_field()
    paths_removed: List[DataPath] = custom_field(
        default_factory=list, mm_field=ListField(DataPathField())
    )


@dataclass
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

    days_since_last_accessed: float = custom_field(default=0, mm_field=FloatField())
    max_depth: Optional[int] = custom_field(default=None, mm_field=IntegerField())
    min_depth: Optional[int] = custom_field(default=None, mm_field=IntegerField())
    min_size_bytes_allowed: int = custom_field(default=0, mm_field=IntegerField())
    current_time: datetime = custom_field(
        default_factory=get_current_time, mm_field=CustomAwareDateTime()
    )


@dataclass
class OutdatedDataPathScannerResponse(SchemaModel):
    """Response containing outdated data paths.

    Attributes:
        paths: List of paths identified as outdated.
    """

    paths: List[DataPath] = custom_field(default_factory=list, mm_field=ListField(DataPathField()))


@dataclass
class GetDataPathStatsRequest(WithDataPath):
    """Request for getting statistics about a data path.

    Attributes:
        path: The data path to get statistics for.
    """

    pass


@dataclass
class GetDataPathStatsResponse(WithDataPath):
    """Response containing data path statistics.

    Attributes:
        path: The data path.
        path_stats: Statistics for the path.
        children: Statistics for child paths keyed by name.
    """

    path_stats: PathStats = custom_field(mm_field=PathStats.as_mm_field())
    children: Dict[str, PathStats] = custom_field(
        mm_field=DictField(keys=StringField(), values=PathStats.as_mm_field())
    )
