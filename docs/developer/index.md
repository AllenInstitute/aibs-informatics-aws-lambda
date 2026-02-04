# Developer Guide

This guide provides information for developers who want to contribute to the AIBS Informatics AWS Lambda library.

## Development Setup

### Prerequisites

- Python 3.11 or higher
- Git
- Make (optional, but recommended)
- Docker (for testing Lambda containers)

### Clone the Repository

```bash
git clone https://github.com/AllenInstitute/aibs-informatics-aws-lambda.git
cd aibs-informatics-aws-lambda
```

### Install Dependencies

Using make:

```bash
make install
```

Using uv (manual):

```bash
uv sync --group dev
```

## Project Structure

```
aibs-informatics-aws-lambda/
├── src/
│   └── aibs_informatics_aws_lambda/
│       ├── main.py                 # CLI entry point
│       ├── common/                 # Base classes and utilities
│       └── handlers/               # Lambda handler implementations
├── test/                           # Test files
├── docker/                         # Docker configuration
├── docs/                           # Documentation
├── pyproject.toml                  # Project configuration
└── Makefile                        # Build automation
```

## Creating a New Handler

### Basic Handler

```python
from aibs_informatics_aws_lambda.common.handler import LambdaHandler

class MyCustomHandler(LambdaHandler):
    """Custom handler for processing events."""
    
    def handle(self, event, context):
        """Process the incoming event.
        
        Args:
            event: The Lambda event payload
            context: The Lambda context object
            
        Returns:
            The response payload
        """
        # Your processing logic here
        return {"status": "success", "data": event}
```

### Handler with Request/Response Models

```python
from dataclasses import dataclass
from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_core.models import SchemaModel

@dataclass
class MyRequest(SchemaModel):
    input_path: str
    output_path: str

@dataclass
class MyResponse(SchemaModel):
    status: str
    files_processed: int

class TypedHandler(LambdaHandler[MyRequest, MyResponse]):
    def handle(self, request: MyRequest, context) -> MyResponse:
        # Process with typed request
        return MyResponse(status="success", files_processed=10)
```

## Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test coverage-server

# Run specific test file
pytest test/aibs_informatics_aws_lambda/handlers/test_data_sync.py
```

## Building Documentation

```bash
# Serve documentation locally
make docs-serve

# Build documentation
make docs-build
```

## Docker Development

The package includes Docker support for Lambda container build:

```bash
# Build the Docker image
make docker-build
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -am 'Add my feature'`)
6. Push to the branch (`git push origin feature/my-feature`)
7. Create a Pull Request

Please see [CONTRIBUTING.md](https://github.com/AllenInstitute/aibs-informatics-aws-lambda/blob/main/CONTRIBUTING.md) for detailed guidelines.
