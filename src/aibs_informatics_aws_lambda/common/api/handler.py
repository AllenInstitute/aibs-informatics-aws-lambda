"""API Gateway Lambda handler base class.

Provides base classes for creating strongly-typed Lambda handlers
that integrate with API Gateway.
"""

__all__ = [
    "ApiLambdaHandler",
]

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Generic, Optional, TypeVar, Union, cast

from aibs_informatics_core.models.api.http_parameters import HTTPParameters
from aibs_informatics_core.models.api.route import ApiRoute
from aibs_informatics_core.models.base import ModelProtocol
from aibs_informatics_core.utils.json import JSON
from aws_lambda_powertools.event_handler.api_gateway import BaseRouter
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.metrics import EphemeralMetrics, Metrics
from aws_lambda_powertools.utilities.data_classes.api_gateway_proxy_event import (
    APIGatewayEventRequestContext,
    APIGatewayProxyEvent,
)
from aws_lambda_powertools.utilities.data_classes.common import (
    APIGatewayEventIdentity,
    BaseProxyEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext

from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_aws_lambda.common.metrics import (
    add_duration_metric,
    add_failure_metric,
    add_success_metric,
)

LambdaEvent = Union[JSON]  # type: ignore  # https://github.com/python/mypy/issues/7866

LambdaHandlerType = Callable[[LambdaEvent, LambdaContext], Optional[JSON]]

API_REQUEST = TypeVar("API_REQUEST", bound=ModelProtocol)
API_RESPONSE = TypeVar("API_RESPONSE", bound=ModelProtocol)


@dataclass  # type: ignore[misc] # mypy #5374
class ApiLambdaHandler(
    LambdaHandler[API_REQUEST, API_RESPONSE],
    ApiRoute[API_REQUEST, API_RESPONSE],
    Generic[API_REQUEST, API_RESPONSE],
):
    """Base class for API Gateway Lambda handlers.

    Combines the LambdaHandler capabilities with API Gateway routing,
    providing automatic request parsing, response formatting, and
    integration with metrics and logging.

    Type Parameters:
        API_REQUEST: The request model type.
        API_RESPONSE: The response model type.

    Example:
        ```python
        @dataclass
        class MyApiHandler(ApiLambdaHandler[MyRequest, MyResponse]):
            @classmethod
            def route_rule(cls) -> str:
                return "/users/{user_id}"

            @classmethod
            def route_method(cls) -> str:
                return "GET"

            def handle(self, request: MyRequest) -> MyResponse:
                return MyResponse(...)
        ```
    """

    _current_event: Optional[BaseProxyEvent] = field(default=None, repr=False)

    def __post_init__(self):
        super().__post_init__()

    @property
    def current_event(self) -> BaseProxyEvent:
        """Get the current API Gateway proxy event.

        Returns:
            The current proxy event being processed.

        Raises:
            ValueError: If no event is currently set.
        """
        if self._current_event is None:
            raise ValueError(f"Current event not set for {self}.")
        return self._current_event

    @current_event.setter
    def current_event(self, value: BaseProxyEvent):
        """Set the current API Gateway proxy event.

        Args:
            value (BaseProxyEvent): The proxy event to set.
        """
        self._current_event = value

    @property
    def api_gateway_proxy_event(self) -> APIGatewayProxyEvent:
        """Get the current event as an APIGatewayProxyEvent.

        Returns:
            The current event as an APIGatewayProxyEvent instance.
        """
        if isinstance(self.current_event, APIGatewayProxyEvent):
            return self.current_event
        return APIGatewayProxyEvent(self.current_event._data)

    @property
    def api_gateway_proxy_request_context(self) -> APIGatewayEventRequestContext:
        """Get the request context from the current event.

        Returns:
            The API Gateway request context.
        """
        return self.api_gateway_proxy_event.request_context

    @property
    def api_gateway_event_identity(self) -> APIGatewayEventIdentity:
        """Get the identity information from the request context.

        Returns:
            The API Gateway event identity.
        """
        return self.api_gateway_proxy_request_context.identity

    @property
    def api_gateway_caller(self) -> str:
        """Get the caller identifier from the event identity.

        Returns:
            The caller or user identifier, or 'Unknown' if not available.
        """
        return (
            self.api_gateway_event_identity.caller
            or self.api_gateway_event_identity.user
            or "Unknown"
        )

    @classmethod
    def add_to_router(
        cls,
        router: BaseRouter,
        *args,
        logger: Optional[Logger] = None,
        metrics: Optional[Union[EphemeralMetrics, Metrics]] = None,
        **kwargs,
    ) -> Callable:
        """Register this handler with an API Gateway router.

        Creates a route handler function and registers it with the router
        using the handler's route rule and method.

        Args:
            router (BaseRouter): The router to register the handler with.
            *args: Additional arguments passed to the handler constructor.
            logger (Optional[Logger]): Optional logger instance. If None, creates a new one.
            metrics (Optional[Union[EphemeralMetrics, Metrics]]): Optional metrics instance.
                If None, creates a new one.
            **kwargs: Additional keyword arguments passed to the handler constructor.

        Returns:
            The registered gateway handler function.
        """
        logger = logger or cls.get_logger(service=cls.service_name())
        metrics = metrics or cls.get_metrics()

        @metrics.log_metrics
        @router.route(rule=cls.route_rule(), method=cls.route_method())
        def gateway_handler(logger=logger, metrics=metrics, **route_parameters) -> Any:
            """Generic gateway handler"""
            start = datetime.now()
            try:
                metrics.add_dimension(name="route", value=cls.route_rule())
                metrics.add_dimension(name="handler", value=cls.handler_name())

                logger.info(f"Handling {router.current_event.raw_event} event.")

                cls._parse_event_headers(router.current_event, logger)

                request = cls._parse_event(
                    router.current_event, route_parameters, cast(logging.Logger, logger)
                )

                logger.debug(f"Getting dict from {request}")
                event = request.to_dict()

                logger.info(f"Constructed following event from HTTP request: {event}")

                lambda_handler = cls.get_handler(
                    *args, _current_event=router.current_event, **kwargs
                )

                logger.info("Route handler method constructed. Invoking")
                response = lambda_handler(event, router.lambda_context)
                add_success_metric(metrics=metrics)
                add_duration_metric(start=start, metrics=metrics)
                return response
            except Exception as e:
                add_failure_metric(metrics=metrics)
                add_duration_metric(start=start, metrics=metrics)
                raise e

        return gateway_handler

    @classmethod
    def _parse_event(
        cls, event: BaseProxyEvent, route_parameters: Dict[str, Any], logger: logging.Logger
    ) -> API_REQUEST:
        """Parse an API Gateway event into a request object.

        Extracts route parameters, query parameters, and request body
        from the event and constructs the typed request object.

        Args:
            event (BaseProxyEvent): The API Gateway proxy event.
            route_parameters (Dict[str, Any]): The route path parameters.
            logger (logging.Logger): Logger for debug output.

        Returns:
            The parsed request object.
        """
        logger.info("parsing event.")
        stringified_route_params = route_parameters
        stringified_query_params = event.query_string_parameters
        stringified_request_body = event.json_body if event.body else None

        logger.info(
            f"Found stringified route parameters = '{stringified_route_params}', "
            f"stringified query parameters = {stringified_query_params}, "
            f"stringified request body = {stringified_request_body}"
        )

        http_parameters = HTTPParameters.from_http_request(
            stringified_route_params=route_parameters,
            stringified_query_params=stringified_query_params,
            stringified_request_body=stringified_request_body,
        )
        logger.debug(f"Constructed following HTTP Parameters: {http_parameters}")

        logger.debug("Converting HTTP Parameters to request object")
        request = cls.get_request_from_http_parameters(http_parameters)
        return request

    @classmethod
    def _parse_event_headers(cls, event: BaseProxyEvent, logger: logging.Logger):
        logger.info("Parsing and validating event headers")
        cls.validate_headers(event.headers)
        config = cls.resolve_request_config(event.headers)
        try:
            if config.service_log_level:
                logger.info(f"Setting log level to {config.service_log_level}")
                logger.setLevel(config.service_log_level)
        except Exception as e:
            logger.warning(f"Failed to set log level to {config.service_log_level}: {e}")

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(route={self.route_rule()}, method={self.route_method()})"
        )
