from aibs_informatics_aws_lambda.handlers.notifications.router import NotificationRouter
from test.aibs_informatics_aws_lambda.base import LambdaHandlerTestCase


class NotificationRouterTests(LambdaHandlerTestCase):
    def setUp(self) -> None:
        super().setUp()

    def test_notification_router(self):
        router = NotificationRouter()
