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
from dataclasses import dataclass
from aibs_informatics_core.models.base import SchemaModel
from aibs_informatics_aws_lambda.common.handler import LambdaHandler

@dataclass
class MyRequest(SchemaModel):
    name: str

@dataclass
class MyResponse(SchemaModel):
    message: str

class MyHandler(LambdaHandler[MyRequest, MyResponse]):
    def handle(self, request: MyRequest) -> MyResponse:
        return MyResponse(message=f"Hello, {request.name}!")

# Create handler function for AWS Lambda
handler = MyHandler.get_handler()
```

### API Gateway Handlers

For API Gateway integrations, use `ApiLambdaHandler`:

```python
from dataclasses import dataclass
from aibs_informatics_core.models.base import SchemaModel
from aibs_informatics_aws_lambda.common.api.handler import ApiLambdaHandler

@dataclass
class UserRequest(SchemaModel):
    user_id: str

@dataclass
class UserResponse(SchemaModel):
    name: str
    email: str

@dataclass
class GetUserHandler(ApiLambdaHandler[UserRequest, UserResponse]):
    @classmethod
    def route_rule(cls) -> str:
        return "/users/{user_id}"

    @classmethod
    def route_method(cls) -> str:
        return "GET"

    def handle(self, request: UserRequest) -> UserResponse:
        # Fetch user and return response
        return UserResponse(name="John Doe", email="john@example.com")
```

### Using Built-in Handlers

The library provides several ready-to-use handlers for common tasks:

```python
from aibs_informatics_aws_lambda.handlers.data_sync.operations import GetJSONFromFileHandler

# Use directly as a Lambda handler
handler = GetJSONFromFileHandler().get_handler()
```

## Next Steps

- Learn about [CLI Usage](cli-usage.md) for local testing
- Explore the [API Reference](../api/index.md) for detailed documentation
- See the [Developer Guide](../developer/index.md) for contribution guidelines
