# Notification Handlers

Handlers for notification routing and delivery.

## Overview

| Module | Description |
|--------|-------------|
| [Router](router.md) | `NotificationRouter` for routing notifications |
| [Model](model.md) | Data models for notifications |
| [Notifiers](notifiers/base.md) | Base notifier and implementations |

## Notifiers

| Notifier | Description |
|----------|-------------|
| [SES](notifiers/ses.md) | Send notifications via Amazon SES |
| [SNS](notifiers/sns.md) | Send notifications via Amazon SNS |

## Quick Start

```python
from aibs_informatics_aws_lambda.handlers.notifications.router import NotificationRouter

handler = NotificationRouter().handler
```
