# Data Sync Handlers

Handlers for data synchronization and file system operations.

## Overview

| Module | Description |
|--------|-------------|
| [Operations](operations.md) | Data sync operations (read, write, sync) |
| [File System](file-system.md) | File system operations (list, stats, remove) |
| [Model](model.md) | Data models for data sync operations |

## Available Handlers

### Operations

- `GetJSONFromFileHandler` - Retrieves JSON data from a file
- `PutJSONToFileHandler` - Writes JSON data to a file
- `DataSyncHandler` - Simple data sync task
- `BatchDataSyncHandler` - Handles batch of data sync tasks
- `PrepareBatchDataSyncHandler` - Prepares batch data sync tasks

### File System

- `GetDataPathStatsHandler` - Retrieves statistics about data paths
- `ListDataPathsHandler` - Lists data paths
- `OutdatedDataPathScannerHandler` - Scans for outdated data paths
- `RemoveDataPathsHandler` - Removes data paths

## Quick Start

```python
from aibs_informatics_aws_lambda.handlers.data_sync.operations import GetJSONFromFileHandler

handler = GetJSONFromFileHandler().handler
```
