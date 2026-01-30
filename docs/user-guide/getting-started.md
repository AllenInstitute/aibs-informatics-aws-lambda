# Getting Started

This guide will help you get started with the AIBS Informatics AWS Lambda library.

## Installation

### Using pip

```bash
pip install aibs-informatics-aws-lambda
```

### Using uv

```bash
uv add aibs-informatics-aws-lambda
```

## Basic Concepts

### Lambda Handlers

The `LambdaHandler` class provides a base class for creating strongly typed lambda functions with features like serialization/deserialization, logging, and metrics.

```python
from aibs_informatics_aws_lambda.common.handler import LambdaHandler

class MyHandler(LambdaHandler):
    def handle(self, event, context):
        # Process the event
        return {"status": "success"}

# Create handler function for AWS Lambda
handler = MyHandler().handler
```

### API Gateway Handlers

For API Gateway integrations, use `ApiLambdaHandler`:

```python
from aibs_informatics_aws_lambda.common.api.handler import ApiLambdaHandler

class MyApiHandler(ApiLambdaHandler):
    def handle(self, event, context):
        # Process API Gateway event
        return {"statusCode": 200, "body": "Hello World"}
```

### Using Built-in Handlers

The library provides several ready-to-use handlers for common tasks:

```python
from aibs_informatics_aws_lambda.handlers.data_sync.operations import GetJSONFromFileHandler

# Use directly as a Lambda handler
handler = GetJSONFromFileHandler().handler
```

## Next Steps

- Learn about [CLI Usage](cli-usage.md) for local testing
- Explore the [API Reference](../api/index.md) for detailed documentation
- See the [Developer Guide](../developer/index.md) for contribution guidelines
