# AIBS Informatics AWS Lambda

[![Build Status](https://github.com/AllenInstitute/aibs-informatics-aws-lambda/actions/workflows/build.yml/badge.svg)](https://github.com/AllenInstitute/aibs-informatics-aws-lambda/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/AllenInstitute/aibs-informatics-aws-lambda/graph/badge.svg?token=SEHNFMIX4G)](https://codecov.io/gh/AllenInstitute/aibs-informatics-aws-lambda)

---

This is a base package that can be used standalone with some core lambda functionality or as a dependency. 


## Package Overview

The package contains  several classes and functions that make it easy to create strongly typed lambda functions with many nice-to-have features (serialization/deserialization, easy to add metrics, utilities to create batch sqs and dynamo db event bridge processing ). In addition to these base classes, you can also use a collection of general purpose lambda handler classes. 

### Base Classes and Functions 

#### `LambdaHandler`

The [`LambdaHandler`](src/aibs_informatics_aws_lambda/common/handler.py) class provides a base class for creating strongly typed lambda functions with features like serialization/deserialization, logging, and metrics.


#### `ApiLambdaHandler` and `ApiResolverBuilder`

These classes extend the `LambdaHandler` class and provide a way to create strongly typed lambda functions that can be used as API Gateway endpoints.

- [`ApiLambdaHandler`](src/aibs_informatics_aws_lambda/common/api/handler.py): A base class for API Gateway handlers.
- [`ApiResolverBuilder`](src/aibs_informatics_aws_lambda/common/api/resolver.py): A utility class for building API Gateway resolvers.


### Standalone Lambda Classes in this Package

Most of these lambda functions are found under [src/aibs_informatics_aws_lambda/handlers/](./src/aibs_informatics_aws_lambda/handlers/)

#### AWS Batch Functions

- [`CreateDefinitionAndPrepareArgsHandler`](src/aibs_informatics_aws_lambda/handlers/batch/create.py): Handles the creation and preparation of AWS Batch job definitions.
- [`PrepareBatchDataSyncHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/operations.py): Prepares data synchronization tasks for AWS Batch.

#### Data Sync Functions

##### Data Sync Operations (Writing, Reading and Syncing Data)
- [`GetJSONFromFileHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/operations.py): Retrieves JSON data from a file.
- [`PutJSONToFileHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/operations.py): Writes JSON data to a file.
- [`DataSyncHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/operations.py): Simple data sync task.
- [`BatchDataSyncHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/operations.py): Handles batch of data sync tasks.
- [`PrepareBatchDataSyncHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/operations.py): Taking a data sync request, it analyzes and generates multiple batches of data sync tasks to evenly distribute the load across multiple batch data sync tasks. 


#### Data Sync File System Functions (Managing Data Paths)
- [`GetDataPathStatsHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/file_system.py): Retrieves statistics about data paths.
- [`ListDataPathsHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/file_system.py): Lists data paths.
- [`OutdatedDataPathScannerHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/file_system.py): Scans for outdated data paths.
- [`RemoveDataPathsHandler`](src/aibs_informatics_aws_lambda/handlers/data_sync/file_system.py): Removes data paths.

#### Demand Functions

- [`PrepareDemandScaffoldingHandler`](src/aibs_informatics_aws_lambda/handlers/demand/scaffolding.py): Prepares scaffolding for demand execution.

#### Notification Functions

- [`NotificationRouter`](src/aibs_informatics_aws_lambda/handlers/notifications/router.py): Routes notifications to the appropriate notifier.
- [`SESNotifier`](src/aibs_informatics_aws_lambda/handlers/notifications/notifiers/ses.py): Sends notifications via Amazon SES.
- [`SNSNotifier`](src/aibs_informatics_aws_lambda/handlers/notifications/notifiers/sns.py): Sends notifications via Amazon SNS.


#### ECR Image Replicator

- [`ImageReplicatorHandler`](src/aibs_informatics_aws_lambda/handlers/ecr/replicate_image.py): Handles the replication of ECR images between repositories using the [`ECRImageReplicator`](https://github.com/AllenInstitute/aibs-informatics-aws-utils/tree/main/src/aibs_informatics_aws_utils/ecr/image_replicator.py).



### CLI Invocation

With this package, you can also invoke lambda functions from the command line. The CLI executable is installed as `handle-lambda-request` and can be used to invoke lambda functions with payloads that can be specified as JSON, files, or S3 objects. 

```
usage: handle-lambda-request [-h] [--handler-qualified-name HANDLER_QUALIFIED_NAME] [--payload PAYLOAD] [--response-location RESPONSE_LOCATION]

CLI AWS Lambda Handler

options:
  -h, --help            show this help message and exit
  --handler-qualified-name HANDLER_QUALIFIED_NAME, --handler-name HANDLER_QUALIFIED_NAME, --handler HANDLER_QUALIFIED_NAME
                        handler function qualified name. If not provided, will try to load from ('AWS_LAMBDA_FUNCTION_HANDLER', '_HANDLER') env variables
  --payload PAYLOAD, --event PAYLOAD, -e PAYLOAD
                        event payload of function. If not provided, will try to load from AWS_LAMBDA_EVENT_PAYLOAD env variable
  --response-location RESPONSE_LOCATION, -o RESPONSE_LOCATION
                        optional response location to store response at. can be S3 or local file. If not provided, will load from AWS_LAMBDA_EVENT_RESPONSE_LOCATION env variable.
```

#### Examples

##### Invoking a Lambda Function with a JSON Payload

```bash
handle-lambda-request --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler --payload '{"path": "/path/to/file.json"}' --response-location /tmp/response.json
```

##### Invoking a Lambda Function with a JSON Payload from a File

```bash
handle-lambda-request --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler --payload-file /path/to/payload.json --response-location /tmp/response.json
```

##### Invoking a Lambda Function with a JSON Payload from S3 and Saving the Response to S3

```bash
handle-lambda-request --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler --payload-file s3://bucket/key/payload.json --response-location s3://bucket/key/response.json
```

##### Invoking a Lambda Function with environment variables

```bash
AWS_LAMBDA_EVENT_PAYLOAD='{"path": "/path/to/file.json"}'
AWS_LAMBDA_EVENT_RESPONSE_LOCATION='/tmp/response.json'
handle-lambda-request --handler-qualified-name aibs_informatics_aws_lambda.handlers.data_sync.operations.GetJSONFromFileHandler
```


## Testing

The package includes comprehensive tests for all handlers, which can be found under the [test](test) directory.

## Contributing

Any and all PRs are welcome. Please see [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## Licensing

This software is licensed under the Allen Institute Software License, which is the 2-clause BSD license plus a third clause that prohibits redistribution and use for commercial purposes without further permission. For more information, please visit [Allen Institute Terms of Use](https://alleninstitute.org/terms-of-use/).