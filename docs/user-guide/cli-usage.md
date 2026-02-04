# CLI Usage

The `aibs-informatics-aws-lambda` package provides a CLI tool for invoking lambda functions locally.

## Command Overview

```bash
handle-lambda-request [-h] [--handler-qualified-name HANDLER_QUALIFIED_NAME] [--payload PAYLOAD] [--response-location RESPONSE_LOCATION]
```

### Options

| Option | Description |
|--------|-------------|
| `--handler-qualified-name`, `--handler-name`, `--handler` | Handler function qualified name. If not provided, will try to load from `AWS_LAMBDA_FUNCTION_HANDLER` or `_HANDLER` env variables |
| `--payload`, `--event`, `-e` | Event payload of function. If not provided, will try to load from `AWS_LAMBDA_EVENT_PAYLOAD` env variable |
| `--response-location`, `-o` | Optional response location to store response at. Can be S3 or local file. If not provided, will load from `AWS_LAMBDA_EVENT_RESPONSE_LOCATION` env variable |

## Examples

### Invoking with JSON Payload

```bash
handle-lambda-request \
    --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler \
    --payload '{"path": "/path/to/file.json"}' \
    --response-location /tmp/response.json
```

### Invoking with Payload from File

```bash
handle-lambda-request \
    --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler \
    --payload file:///path/to/payload.json \
    --response-location /tmp/response.json
```

### Invoking with Payload from S3

```bash
handle-lambda-request \
    --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler \
    --payload s3://my-bucket/payload.json \
    --response-location s3://my-bucket/response.json
```

### Using Environment Variables

```bash
export AWS_LAMBDA_FUNCTION_HANDLER="aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler"
export AWS_LAMBDA_EVENT_PAYLOAD='{"path": "/path/to/file.json"}'
export AWS_LAMBDA_EVENT_RESPONSE_LOCATION="/tmp/response.json"

handle-lambda-request
```

## Available Handlers

### Data Sync Operations

| Handler | Description |
|---------|-------------|
| `aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler` | Retrieves JSON data from a file |
| `aibs_informatics_aws_lambda.handlers.data_sync.operations.PutJSONToFileHandler` | Writes JSON data to a file |
| `aibs_informatics_aws_lambda.handlers.data_sync.operations.DataSyncHandler` | Simple data sync task |
| `aibs_informatics_aws_lambda.handlers.data_sync.operations.BatchDataSyncHandler` | Handles batch of data sync tasks |
| `aibs_informatics_aws_lambda.handlers.data_sync.operations.PrepareBatchDataSyncHandler` | Prepares batch data sync tasks |

### Data Sync File System

| Handler | Description |
|---------|-------------|
| `aibs_informatics_aws_lambda.handlers.data_sync.file_system.GetDataPathStatsHandler` | Retrieves statistics about data paths |
| `aibs_informatics_aws_lambda.handlers.data_sync.file_system.ListDataPathsHandler` | Lists data paths |
| `aibs_informatics_aws_lambda.handlers.data_sync.file_system.OutdatedDataPathScannerHandler` | Scans for outdated data paths |
| `aibs_informatics_aws_lambda.handlers.data_sync.file_system.RemoveDataPathsHandler` | Removes data paths |

### AWS Batch

| Handler | Description |
|---------|-------------|
| `aibs_informatics_aws_lambda.handlers.batch.create.CreateDefinitionAndPrepareArgsHandler` | Creates and prepares AWS Batch job definitions |

### Demand

| Handler | Description |
|---------|-------------|
| `aibs_informatics_aws_lambda.handlers.demand.scaffolding.PrepareDemandScaffoldingHandler` | Prepares scaffolding for demand execution |

### ECR

| Handler | Description |
|---------|-------------|
| `aibs_informatics_aws_lambda.handlers.ecr.replicate_image.ImageReplicatorHandler` | Replicates ECR images between repositories |

### Notifications

| Handler | Description |
|---------|-------------|
| `aibs_informatics_aws_lambda.handlers.notifications.router.NotificationRouter` | Routes notifications to appropriate notifier |
