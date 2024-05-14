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
from aws_lambda_powertools.tracing import Tracer
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools.utilities.typing import LambdaContext

from aibs_informatics_aws_lambda.common.api.handler import ApiLambdaHandler
from aibs_informatics_aws_lambda.common.logging import LoggingMixins
from aibs_informatics_aws_lambda.common.metrics import MetricsMixins

LambdaEvent = Union[JSON]  # type: ignore  # https://github.com/python/mypy/issues/7866

LambdaHandlerType = Callable[[LambdaEvent, LambdaContext], JSONObject]


@dataclass
class ApiResolverBuilder(LoggingMixins, MetricsMixins, PostInitMixin):
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
        pass

    def update_logging_level(self, event: APIGatewayProxyEvent) -> None:
        """Update the logging level based on the event headers"""
        if log_level := event.headers.get("X-Log-Level"):
            try:
                self.logger.setLevel(log_level)
            except Exception as e:
                self.logger.warning(f"Failed to set log level to {log_level}: {e}")

    def handle_not_found(self, e: NotFoundError) -> Response:
        msg = f"Could not find route {self.app.current_event.path}: {e.msg}"
        self.logger.exception(msg)
        self.metrics.add_count_metric("RouteNotFound", 1)
        return Response(status_code=418, content_type=content_types.TEXT_PLAIN, body=msg)

    def handle(self, event: LambdaEvent, context: LambdaContext) -> JSONObject:
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
        """Dynamically adds all API Lambda handlers under package root to app

        Args:
            router (BaseRouter): The router to which handlers add a route
            root_mod_or_pkg (ModuleType): the root package or module under which are the
                targeted handler classes to add to this router.
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
    target_api_handler_classes = get_target_handler_classes(target_module)

    # Add each lambda handler to the route.
    for api_handler_class in target_api_handler_classes:
        api_handler_class.add_to_router(router, logger=logger, metrics=metrics)


def get_target_handler_classes(target_module: ModuleType) -> List[ApiLambdaHandler]:
    """Get all ApiLambdaHandler subclasses in the target module or package

    Returns:
        List[ApiLambdaHandler]: All ApiLambdaHandler subclasses in this package
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
