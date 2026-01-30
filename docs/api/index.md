# API Reference

Welcome to the AIBS Informatics AWS Lambda API Reference. This section provides detailed documentation for all modules, classes, and functions in the library.

## Module Overview

### Core Modules

| Module | Description |
|--------|-------------|
| [Main](main.md) | CLI entry point and main execution logic |
| [Common](common/index.md) | Base classes, utilities, and shared components |

### Handlers

| Module | Description |
|--------|-------------|
| [Batch](handlers/batch/index.md) | AWS Batch job definition and execution handlers |
| [Data Sync](handlers/data-sync/index.md) | Data synchronization and file system handlers |
| [Demand](handlers/demand/index.md) | Demand execution scaffolding handlers |
| [ECR](handlers/ecr/index.md) | ECR image replication handlers |
| [Notifications](handlers/notifications/index.md) | Notification routing and delivery handlers |

## Quick Links

### Base Classes

- [`LambdaHandler`](common/handler.md) - Base class for all Lambda handlers
- [`ApiLambdaHandler`](common/api/handler.md) - Base class for API Gateway handlers
- [`ApiResolverBuilder`](common/api/resolver.md) - Utility for building API resolvers

### Common Handlers

- [`GetJSONFromFileHandler`](handlers/data-sync/operations.md) - Read JSON from file
- [`PutJSONToFileHandler`](handlers/data-sync/operations.md) - Write JSON to file
- [`DataSyncHandler`](handlers/data-sync/operations.md) - Synchronize data
- [`NotificationRouter`](handlers/notifications/router.md) - Route notifications
