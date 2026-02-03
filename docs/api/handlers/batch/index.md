# Batch Handlers

Handlers for AWS Batch job definition and execution.

## Overview

| Module | Description |
|--------|-------------|
| [Create](create.md) | `CreateDefinitionAndPrepareArgsHandler` for creating and preparing AWS Batch job definitions |
| [Model](model.md) | Data models for batch operations |

## Quick Start

```python
from aibs_informatics_aws_lambda.handlers.batch.create import CreateDefinitionAndPrepareArgsHandler

handler = CreateDefinitionAndPrepareArgsHandler().get_handler()
```
