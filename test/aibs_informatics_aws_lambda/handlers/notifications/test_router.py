from datetime import timedelta
from pathlib import Path
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase
from time import sleep
from typing import Tuple, Union

from aibs_informatics_aws_utils.data_sync.file_system import LocalFileSystem, PathStats
from aibs_informatics_core.utils.hashing import uuid_str

from aibs_informatics_aws_lambda.handlers.notifications.model import (
    NotificationContent,
    NotificationRequest,
    NotificationResponse,
)
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.ses import SESNotifier
from aibs_informatics_aws_lambda.handlers.notifications.notifiers.sns import SNSNotifier
from aibs_informatics_aws_lambda.handlers.notifications.router import NotificationRouter


class NotificationRouterTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()

    def test_notification_router(self):
        router = NotificationRouter()
