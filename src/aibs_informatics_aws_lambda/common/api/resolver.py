"""API Gateway resolver builder for Lambda handlers.

Provides utilities for building API Gateway REST resolvers
with automatic handler discovery and registration.
"""

__all__ = [
    "ApiResolverBuilder",
]
import json
from dataclasses import dataclass, field
from datetime import datetime
from traceback import format_exc
from types import ModuleType
from typing import Callable, ClassVar, List, Optional, Union

from aibs_informatics_core.collections import PostInitMixin
from aibs_informatics_core.utils.json import JSON, JSONObject
from aibs_informatics_core.utils.modules import get_all_subclasses, load_all_modules_from_pkg
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, content_types
from aws_lambda_powertools.event_handler.api_gateway import BaseRouter, Response, Router
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from aws_lambda_powertools.event_handler.middlewares import NextMiddleware
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.logging.correlation_paths import API_GATEWAY_REST
from aws_lambda_powertools.metrics import EphemeralMetrics, Metrics
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools.utilities.typing import LambdaContext

from aibs_informatics_aws_lambda.common.api.handler import ApiLambdaHandler
from aibs_informatics_aws_lambda.common.logging import LoggingMixins
from aibs_informatics_aws_lambda.common.metrics import MetricsMixins

LambdaEvent = Union[JSON]  # type: ignore  # https://github.com/python/mypy/issues/7866

LambdaHandlerType = Callable[[LambdaEvent, LambdaContext], JSONObject]


@dataclass
class ApiResolverBuilder(LoggingMixins, MetricsMixins, PostInitMixin):
    """Builder for API Gateway REST resolvers with automatic handler registration.

    Provides a convenient way to build API Gateway resolvers with built-in
    middleware for validation, logging, and error handling.

    Example:
        ```python
        builder = ApiResolverBuilder()
        builder.add_handlers(my_handlers_module)
        handler = builder.get_lambda_handler()
        ```
    """

    app: APIGatewayRestResolver = field(default_factory=APIGatewayRestResolver)

    metric_name_prefix: ClassVar[str] = "ApiResolver"

    def __post_init__(self):
        super().__post_init__()
        self.logger = self.get_logger(service=self.service_name(), add_to_root=False)

        # Adding default middleware
        def validation_middleware(
            app: APIGatewayRestResolver, next_middleware: NextMiddleware
        ) -> Response:
            try:
                self.validate_event(app.current_event)
            except Exception as e:
                return Response(
                    status_code=401,
                    content_type=content_types.TEXT_PLAIN,
                    body=f"Failed to validate event: {e}",
                )
            else:
                return next_middleware(app)

        def logging_middleware(
            app: APIGatewayRestResolver, next_middleware: NextMiddleware
        ) -> Response:
            self.update_logging_level(app.current_event)
            return next_middleware(app)

        self.app.use(middlewares=[validation_middleware, logging_middleware])

        # Adding default exception handlers
        self.app.exception_handler(Exception)(self.handle_exception)
        self.app.not_found(self.handle_not_found)

    def handle_exception(self, e: Exception):
        """Handle uncaught exceptions in request processing.

        Args:
            e (Exception): The exception that was raised.

        Returns:
            A Response with status 400 and error details.
        """
        metadata = {"path": self.app.current_event.path}
        self.logger.exception(f"{e}", extra=metadata)
        return Response(
            status_code=400,
            content_type=content_types.APPLICATION_JSON,
            body=json.dumps(
                {
                    "request": self.app.lambda_context.aws_request_id,
                    "error": e.args,
                    "stacktrace": format_exc(),
                },
                indent=True,
            ),
        )

    def validate_event(self, event: APIGatewayProxyEvent) -> None:
        """Validate the incoming API Gateway event.

        Override this method to add custom validation logic.

        Args:
            event (APIGatewayProxyEvent): The API Gateway proxy event to validate.

        Raises:
            Exception: If validation fails.
        """
        pass

    def update_logging_level(self, event: APIGatewayProxyEvent) -> None:
        """Update the logging level based on request headers.

        Checks for an 'X-Log-Level' header and adjusts the logger
        level accordingly.

        Args:
            event (APIGatewayProxyEvent): The API Gateway proxy event.
        """
        if log_level := event.headers.get("X-Log-Level"):
            try:
                self.logger.setLevel(log_level)
            except Exception as e:
                self.logger.warning(f"Failed to set log level to {log_level}: {e}")

    def handle_not_found(self, e: NotFoundError) -> Response:
        """Handle requests to non-existent routes.

        Args:
            e (NotFoundError): The NotFoundError exception.

        Returns:
            A Response with status 418 and error message.
        """
        msg = f"Could not find route {self.app.current_event.path}: {e.msg}"
        self.logger.exception(msg)
        self.metrics.add_count_metric("RouteNotFound", 1)
        return Response(status_code=418, content_type=content_types.TEXT_PLAIN, body=msg)

    def handle(self, event: LambdaEvent, context: LambdaContext) -> JSONObject:
        """Handle an incoming API Gateway event.

        Resolves the event to the appropriate handler and returns the response.

        Args:
            event (LambdaEvent): The Lambda event payload.
            context (LambdaContext): The Lambda context.

        Returns:
            The JSON response from the resolved handler.

        Raises:
            Exception: If handler execution fails.
        """
        start = datetime.now()
        try:
            self.logger.info(f"Handling API Lambda event: {event}")
            response = self.app.resolve(event, context)
            self.metrics.add_success_metric(self.metric_name_prefix)
            self.metrics.add_duration_metric(start, name=self.metric_name_prefix)
            return response
        except Exception as e:
            self.logger.error(f"API Lambda handler failed with following error: {e}")
            self.metrics.add_failure_metric(self.metric_name_prefix)
            self.metrics.add_duration_metric(start, name=self.metric_name_prefix)
            raise e

    def get_lambda_handler(self, *args, **kwargs) -> LambdaHandlerType:
        """Create a Lambda handler function for this resolver.

        Wraps the handle method with logging context injection
        and metrics collection.

        Args:
            *args: Positional arguments (unused).
            **kwargs: Keyword arguments (unused).

        Returns:
            A callable Lambda handler function.
        """
        lambda_handler = self.handle

        lambda_handler = self.logger.inject_lambda_context(correlation_id_path=API_GATEWAY_REST)(
            lambda_handler
        )
        lambda_handler = self.metrics.log_metrics(capture_cold_start_metric=True)(lambda_handler)  # type: ignore

        return lambda_handler

    def add_handlers(
        self,
        target_module: ModuleType,
        router: Optional[BaseRouter] = None,
        prefix: Optional[str] = None,
    ):
        """Dynamically add all API Lambda handlers from a module.

        Discovers all ApiLambdaHandler subclasses in the target module
        and registers them with the router.

        Args:
            target_module (ModuleType): The module containing handler classes.
            router (Optional[BaseRouter]): Optional router to add handlers to. If None with prefix,
                creates a new Router.
            prefix (Optional[str]): Optional URL prefix for all routes in the module.
        """

        if not router and not prefix:
            router = self.app
        elif not router:
            router = Router()

        add_handlers_to_router(
            router=router,
            target_module=target_module,
            logger=self.logger,
            metrics=self.metrics,
        )

        if isinstance(router, Router):
            self.app.include_router(router=router, prefix=prefix)


def add_handlers_to_router(
    router: BaseRouter,
    target_module: ModuleType,
    metrics: Optional[Union[EphemeralMetrics, Metrics]] = None,
    logger: Optional[Logger] = None,
):
    """Add all API handlers from a module to a router.

    Discovers ApiLambdaHandler subclasses in the target module and
    registers each with the router.

    Args:
        router (BaseRouter): The router to register handlers with.
        target_module (ModuleType): The module containing handler classes.
        metrics (Optional[Union[EphemeralMetrics, Metrics]]): Optional metrics collector
            for the handlers.
        logger (Optional[Logger]): Optional logger for the handlers.
    """
    target_api_handler_classes = get_target_handler_classes(target_module)

    # Add each lambda handler to the route.
    for api_handler_class in target_api_handler_classes:
        api_handler_class.add_to_router(router, logger=logger, metrics=metrics)


def get_target_handler_classes(target_module: ModuleType) -> List[ApiLambdaHandler]:
    """Get all ApiLambdaHandler subclasses in a module.

    Recursively loads all modules from the target package and returns
    all ApiLambdaHandler subclasses found.

    Args:
        target_module (ModuleType): The module or package to search.

    Returns:
        A list of ApiLambdaHandler subclasses found in the module.
    """
    # Load modules from package root.
    loaded_modules = load_all_modules_from_pkg(target_module, include_packages=True)

    # Resolve subclasses of GCSApiLambdaHandler found within package root.
    target_module_paths = [
        # Along with loaded modules, we also add the root module
        # to the list of target module paths. Depending on whether
        # the root module is a module or a package, we must resolve
        # the string path differently.
        target_module.__name__,
        getattr(target_module, "__module__", getattr(target_module, "__package__")),
        *list(loaded_modules.keys()),
    ]

    target_api_handler_classes: List[ApiLambdaHandler] = [
        api_handler_class
        for api_handler_class in get_all_subclasses(ApiLambdaHandler, True)  # type: ignore[type-abstract]
        if (getattr(api_handler_class, "__module__") in target_module_paths)
    ]
    return target_api_handler_classes
