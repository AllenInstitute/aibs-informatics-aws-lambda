from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, List, Optional, Union

from aibs_informatics_core.exceptions import ApplicationException
from aibs_informatics_core.models.base import StringField, custom_field
from aibs_informatics_core.utils.json import JSON
from aws_lambda_powertools.utilities.typing import LambdaContext
from aibs_informatics_aws_lambda.handlers.notifications.publishers.base import BasePublisher
from aibs_informatics_aws_lambda.handlers.notifications.publishers.ses import SESPublisher
from aibs_informatics_aws_lambda.handlers.notifications.publishers.sns import SNSPublisher
from aibs_informatics_aws_lambda.handlers.notifications.publishers.model import (
    NOTIFICATION_EVENT,
    PublishRequest,
    PublishResponse,
    PublishResponses,
)

from aibs_informatics_aws_lambda.common.handler import LambdaHandler

LambdaEvent = Union[JSON]  # type: ignore  # https://github.com/python/mypy/issues/7866

LambdaHandlerType = Callable[[LambdaEvent, LambdaContext], Optional[JSON]]


@dataclass
class UnhandledPublishRequestResponse(PublishResponse):
    err_msg: str = custom_field(mm_field=StringField())


@dataclass  # type: ignore[misc] # mypy #5374
class NotificationLambdaHandler(
    LambdaHandler[NOTIFICATION_EVENT, PublishResponses], Generic[NOTIFICATION_EVENT]
):
    """Abstract Base class for notification handlers

    To use this class, create a NotificationEvent model to validate incoming events.
    Create a parse_event class to create PublishRequests.

    PublishRequests are handled with a chain-of-responsibility model. Each publisher
    checks if it can handle the PublishRequest, and if so it handles it, returning a
    PublishReponse. If it can't handle it, the request continues to the next publisher.

    If desired, you may provide a `publishers` list to specify allowed publishers

    """

    publishers: List[BasePublisher] = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        if not self.publishers:
            self.publishers = [SESPublisher(), SNSPublisher()]

    def handle(self, request: NOTIFICATION_EVENT) -> PublishResponses:
        publish_requests: List[PublishRequest] = self.parse_event(event=request)
        responses: List[PublishResponse] = []
        for publish_request in publish_requests:
            for publisher in self.publishers:
                if publisher.should_handle(publish_request):
                    self.logger.info(f"{str(publisher)} handling request {publish_request}")
                    responses.append(publisher.publish(publish_request))
                    break  # TODO: let all publishers attempt to handle?
            else:
                err_msg = f"Could not handle notification request: {publish_request} generated from event: {request}"
                self.logger.warning(err_msg)
                responses.append(
                    PublishResponse(response=err_msg, success=False, notifer="Unhandled")
                )

        errors = [_ for _ in responses if not _.success]
        if errors:
            raise ApplicationException(f"Some publish requests failed. Errors: {errors}")

        return PublishResponses(responses=responses)

    @abstractmethod
    def parse_event(self, event: NOTIFICATION_EVENT) -> List[PublishRequest]:
        raise NotImplementedError("Must implement event parsing logic here")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(env_base={self.env_base})"
