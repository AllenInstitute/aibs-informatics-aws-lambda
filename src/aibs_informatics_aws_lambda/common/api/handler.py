__all__ = [
    "ApiLambdaHandler",
]

import logging
from dataclasses import dataclass, field
from datetime import datetime
from types import ModuleType
from typing import Any, Callable, Dict, Generic, List, Optional, Type, TypeVar, Union, cast

from aibs_informatics_core.models.api.http_parameters import HTTPParameters
from aibs_informatics_core.models.api.route import ApiRoute
from aibs_informatics_core.models.base import ModelProtocol
from aibs_informatics_core.utils.json import JSON
from aibs_informatics_core.utils.modules import get_all_subclasses, load_all_modules_from_pkg
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, content_types
from aws_lambda_powertools.event_handler.api_gateway import BaseRouter
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.metrics import EphemeralMetrics, Metrics
from aws_lambda_powertools.tracing import Tracer
from aws_lambda_powertools.utilities.data_classes.api_gateway_proxy_event import (
    APIGatewayEventRequestContext,
    APIGatewayProxyEvent,
)
from aws_lambda_powertools.utilities.data_classes.common import (
    APIGatewayEventIdentity,
    BaseProxyEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext

from aibs_informatics_aws_lambda.common.base import HandlerMixins
from aibs_informatics_aws_lambda.common.handler import LambdaHandler
from aibs_informatics_aws_lambda.common.logging import LoggingMixins
from aibs_informatics_aws_lambda.common.metrics import (
    MetricsMixins,
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
    _current_event: Optional[BaseProxyEvent] = field(default=None, repr=False)

    def __post_init__(self):
        super().__post_init__()

    @property
    def current_event(self) -> BaseProxyEvent:
        if self._current_event is None:
            raise ValueError(f"Current event not set for {self}.")
        return self._current_event

    @current_event.setter
    def current_event(self, value: BaseProxyEvent):
        self._current_event = value

    @property
    def api_gateway_proxy_event(self) -> APIGatewayProxyEvent:
        if isinstance(self.current_event, APIGatewayProxyEvent):
            return self.current_event
        return APIGatewayProxyEvent(self.current_event._data)

    @property
    def api_gateway_proxy_request_context(self) -> APIGatewayEventRequestContext:
        return self.api_gateway_proxy_event.request_context

    @property
    def api_gateway_event_identity(self) -> APIGatewayEventIdentity:
        return self.api_gateway_proxy_request_context.identity

    @property
    def api_gateway_caller(self) -> str:
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
        logger = logger or cls.get_logger(service=cls.service_name())
        metrics = metrics or cls.get_metrics()

        # TODO: remove args once https://github.com/python/mypy/pull/15133 is released (should be mypy 1.2.1)
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

                logger.info(f"Route handler method constructed. Invoking")
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
        logger.info(f"parsing event.")
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

        logger.debug(f"Converting HTTP Parameters to request object")
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
