# AIBS Informatics AWS Lambda

[![Build Status](https://github.com/AllenInstitute/aibs-informatics-aws-lambda/actions/workflows/build.yml/badge.svg)](https://github.com/AllenInstitute/aibs-informatics-aws-lambda/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/AllenInstitute/aibs-informatics-aws-lambda/graph/badge.svg?token=SEHNFMIX4G)](https://codecov.io/gh/AllenInstitute/aibs-informatics-aws-lambda)

---

## Overview

This is a base package that can be used standalone with some core lambda functionality or as a dependency. It contains several classes and functions that make it easy to create strongly typed lambda functions with many nice-to-have features including:

- Serialization/deserialization
- Easy metrics integration
- Utilities for batch SQS and DynamoDB event bridge processing
- Collection of general purpose lambda handler classes

## Features

### Base Classes

| Class | Description |
|-------|-------------|
| [`LambdaHandler`](api/common/handler.md) | Base class for creating strongly typed lambda functions |
| [`ApiLambdaHandler`](api/common/api/handler.md) | Base class for API Gateway handlers |
| [`ApiResolverBuilder`](api/common/api/resolver.md) | Utility class for building API Gateway resolvers |

### Standalone Lambda Handlers

#### AWS Batch
- `CreateDefinitionAndPrepareArgsHandler` - Handles creation and preparation of AWS Batch job definitions

#### Data Sync
- `GetJSONFromFileHandler` - Retrieves JSON data from a file
- `PutJSONToFileHandler` - Writes JSON data to a file
- `DataSyncHandler` - Simple data sync task
- `BatchDataSyncHandler` - Handles batch of data sync tasks
- `PrepareBatchDataSyncHandler` - Prepares data synchronization for AWS Batch
- `GetDataPathStatsHandler` - Retrieves statistics about data paths
- `ListDataPathsHandler` - Lists data paths
- `OutdatedDataPathScannerHandler` - Scans for outdated data paths
- `RemoveDataPathsHandler` - Removes data paths

#### Demand Execution
- `PrepareDemandScaffoldingHandler` - Prepares scaffolding for demand execution

#### Notifications
- `NotificationRouter` - Routes notifications to appropriate notifier
- `SESNotifier` - Sends notifications via Amazon SES
- `SNSNotifier` - Sends notifications via Amazon SNS

#### ECR
- `ImageReplicatorHandler` - Handles replication of ECR images between repositories

## Quick Start

### Installation

```bash
pip install aibs-informatics-aws-lambda
```

### Basic Usage

```python
from aibs_informatics_aws_lambda.common.handler import LambdaHandler

class MyHandler(LambdaHandler):
    def handle(self, event, context):
        # Process the event
        return {"status": "success"}
```

### CLI Invocation

```bash
handle-lambda-request \
    --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler \
    --payload '{"path": "/path/to/file.json"}' \
    --response-location /tmp/response.json
```

## Contributing

Any and all PRs are welcome. Please see [CONTRIBUTING.md](https://github.com/AllenInstitute/aibs-informatics-aws-lambda/blob/main/CONTRIBUTING.md) for more information.

## License

This software is licensed under the Allen Institute Software License. For more information, please visit [Allen Institute Terms of Use](https://alleninstitute.org/terms-of-use/).
