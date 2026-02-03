# Common

The `common` module provides base classes and utilities for creating Lambda handlers.

## Overview

| Module | Description |
|--------|-------------|
| [Handler](handler.md) | `LambdaHandler` base class |
| [Base](base.md) | Base utilities and helpers |
| [Logging](logging.md) | Logging configuration and utilities |
| [Metrics](metrics.md) | Metrics collection utilities |
| [Models](models.md) | Common data models |
| [API](api/handler.md) | API Gateway handler utilities |

## Quick Start

```python
from aibs_informatics_aws_lambda.common.handler import LambdaHandler

class MyHandler(LambdaHandler):
    def handle(self, event, context):
        return {"status": "success"}

# Export handler for AWS Lambda
handler = MyHandler().get_handler()
```
