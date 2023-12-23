from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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

DataPath = Union[S3Path, EFSPath, Path]
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
    pass


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
