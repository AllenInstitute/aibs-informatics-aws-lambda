# ECR Handlers

Handlers for ECR image operations.

## Overview

| Module | Description |
|--------|-------------|
| [Replicate Image](replicate-image.md) | `ImageReplicatorHandler` for replicating ECR images |

## Quick Start

```python
from aibs_informatics_aws_lambda.handlers.ecr.replicate_image import ImageReplicatorHandler

handler = ImageReplicatorHandler().get_handler()
```
