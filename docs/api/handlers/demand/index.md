# Demand Handlers

Handlers for demand execution scaffolding.

## Overview

| Module | Description |
|--------|-------------|
| [Scaffolding](scaffolding.md) | `PrepareDemandScaffoldingHandler` for preparing demand execution |
| [Context Manager](context-manager.md) | Context management utilities |
| [Model](model.md) | Data models for demand operations |

## Quick Start

```python
from aibs_informatics_aws_lambda.handlers.demand.scaffolding import PrepareDemandScaffoldingHandler

handler = PrepareDemandScaffoldingHandler().get_handler()
```
