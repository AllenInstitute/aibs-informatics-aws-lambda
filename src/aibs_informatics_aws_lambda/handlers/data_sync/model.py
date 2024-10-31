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
DataPathField = lambda *args, **kwargs: UnionField(
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
    path: DataPath = custom_field(mm_field=DataPathField())

    @property
    def efs_path(self) -> Optional[EFSPath]:
        if isinstance(self.path, EFSPath):
            return self.path
        return None

    @property
    def s3_uri(self) -> Optional[S3Path]:
        if isinstance(self.path, S3Path):
            return self.path
        return None

    @property
    def local_path(self) -> Optional[Path]:
        if isinstance(self.path, Path):
            return self.path
        return None


@dataclass
class ListDataPathsRequest(WithDataPath):
    """List Data paths request

    Args:
        path (DataPath): path under which to list files
        include (Optional[str|list[str]]): Optionally can specify regex patterns to filter on what
            to include. If providing multiple options, a path is returned if it matches *any* of
            the include patterns. Exclude patterns override include patterns.
            Defaults to None
        exclude (Optional[str|list[str]]): Optionally can specify regex patterns to filter on what
            to exclude. If providing multiple options, a path is omitted if it matches *any* of
            the exclude patterns. Exclude patterns override include patterns.
            Defaults to None
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
    paths: List[DataPath] = custom_field(default_factory=list, mm_field=ListField(DataPathField()))


@dataclass
class RemoveDataPathsRequest(SchemaModel):
    paths: List[DataPath] = custom_field(default_factory=list, mm_field=ListField(DataPathField()))


@dataclass
class RemoveDataPathsResponse(SchemaModel):
    size_bytes_removed: int = custom_field()
    paths_removed: List[DataPath] = custom_field(
        default_factory=list, mm_field=ListField(DataPathField())
    )


@dataclass
class OutdatedDataPathScannerRequest(WithDataPath):
    days_since_last_accessed: float = custom_field(default=7.0, mm_field=FloatField())
    max_depth: Optional[int] = custom_field(default=None, mm_field=IntegerField())
    min_depth: Optional[int] = custom_field(default=None, mm_field=IntegerField())
    min_size_bytes_allowed: int = custom_field(default=0, mm_field=IntegerField())
    current_time: datetime = custom_field(
        default_factory=get_current_time, mm_field=CustomAwareDateTime()
    )


@dataclass
class OutdatedDataPathScannerResponse(SchemaModel):
    paths: List[DataPath] = custom_field(default_factory=list, mm_field=ListField(DataPathField()))


@dataclass
class GetDataPathStatsRequest(WithDataPath):
    pass


@dataclass
class GetDataPathStatsResponse(WithDataPath):
    path_stats: PathStats = custom_field(mm_field=PathStats.as_mm_field())
    children: Dict[str, PathStats] = custom_field(
        mm_field=DictField(keys=StringField(), values=PathStats.as_mm_field())
    )
