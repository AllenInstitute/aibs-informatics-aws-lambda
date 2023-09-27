from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from aibs_informatics_aws_utils.data_sync.file_system import PathStats
from aibs_informatics_core.models.base import (
    CustomAwareDateTime,
    DictField,
    FloatField,
    IntegerField,
    ListField,
    PathField,
    SchemaModel,
    StringField,
    custom_field,
)
from aibs_informatics_core.utils.time import get_current_time


@dataclass
class RemoveEFSPathsRequest(SchemaModel):
    paths: List[Path] = custom_field(default_factory=list, mm_field=ListField(PathField()))
    efs_mount_path: Optional[Path] = custom_field(default=None, mm_field=PathField())


@dataclass
class RemoveEFSPathsResponse(SchemaModel):
    size_bytes_removed: int = custom_field()


@dataclass
class OutdatedEFSPathScannerRequest(SchemaModel):
    path: Optional[Path] = custom_field(default=None, mm_field=PathField())
    efs_mount_path: Optional[Path] = custom_field(default=None, mm_field=PathField())
    days_since_last_accessed: float = custom_field(default=7.0, mm_field=FloatField())
    max_depth: Optional[int] = custom_field(default=None, mm_field=IntegerField())
    min_depth: Optional[int] = custom_field(default=None, mm_field=IntegerField())
    # The minimum EFS total size (in bytes) we want to maintain in order to qualify
    # for a specific EFS IO throughput.
    # For more details see: https://docs.aws.amazon.com/efs/latest/ug/performance.html
    min_efs_size_bytes: int = custom_field(default=0, mm_field=IntegerField())
    current_time: datetime = custom_field(
        default_factory=get_current_time, mm_field=CustomAwareDateTime()
    )


@dataclass
class OutdatedEFSPathScannerResponse(SchemaModel):
    paths: List[Path] = custom_field(default_factory=list, mm_field=ListField(PathField()))
    efs_mount_path: Optional[Path] = custom_field(default=None, mm_field=PathField())


@dataclass
class GetEFSPathStatsRequest(SchemaModel):
    path: Path = custom_field(mm_field=PathField())
    efs_mount_path: Optional[Path] = custom_field(default=None, mm_field=PathField())


@dataclass
class GetEFSPathStatsResponse(SchemaModel):
    efs_mount_path: Path = custom_field(mm_field=PathField())
    path: Path = custom_field(mm_field=PathField())
    path_stats: PathStats = custom_field(mm_field=PathStats.as_mm_field())
    children: Dict[str, PathStats] = custom_field(
        mm_field=DictField(keys=StringField(), values=PathStats.as_mm_field())
    )


@dataclass
class ListEFSPathsRequest(SchemaModel):
    path: Path = custom_field(mm_field=PathField())
    efs_mount_path: Optional[Path] = custom_field(default=None, mm_field=PathField())


@dataclass
class ListEFSPathsResponse(SchemaModel):
    paths: List[Path] = custom_field(default_factory=list, mm_field=ListField(PathField()))
    efs_mount_path: Optional[Path] = custom_field(default=None, mm_field=PathField())
