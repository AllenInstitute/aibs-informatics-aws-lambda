"""Data models for Lambda handler configuration and execution.

Provides models for Lambda context, handler requests, and serialization utilities.
"""

import inspect
from dataclasses import dataclass, field
from typing import Annotated, cast

from aibs_informatics_aws_utils.constants.lambda_ import (
    AWS_LAMBDA_FUNCTION_ARN_KEY,
    AWS_LAMBDA_FUNCTION_MEMORY_SIZE_KEY,
    AWS_LAMBDA_FUNCTION_NAME_KEY,
    AWS_LAMBDA_FUNCTION_REQUEST_ID_KEY,
    AWS_LAMBDA_FUNCTION_VERSION_KEY,
    AWS_LAMBDA_LOG_GROUP_NAME_KEY,
    AWS_LAMBDA_LOG_STREAM_NAME_KEY,
    DEFAULT_AWS_LAMBDA_FUNCTION_NAME,
)
from aibs_informatics_aws_utils.core import get_account_id, get_region
from aibs_informatics_core.models.base import PydanticBaseModel
from aibs_informatics_core.utils.modules import as_module_type, get_qualified_name
from aibs_informatics_core.utils.os_operations import get_env_var
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.typing.lambda_client_context import LambdaClientContext
from aws_lambda_powertools.utilities.typing.lambda_cognito_identity import LambdaCognitoIdentity
from pydantic import BeforeValidator, PlainSerializer

from aibs_informatics_aws_lambda.common.handler import (
    LambdaEvent,
    LambdaHandler,
    LambdaHandlerType,
)
from aibs_informatics_aws_lambda.common.logging import get_service_logger

logger = get_service_logger(__name__)


AWS_LAMBDA_FUNCTION_NAME = "unknown"


@dataclass
class DefaultLambdaContext(LambdaContext):
    """Standard implementation of LambdaContext for non-Lambda environments.

    Provides a mock Lambda context for running handlers outside of AWS Lambda,
    such as in Docker containers or for local testing. All fields can be
    configured via environment variables.

    """

    _function_name: str = field(
        default_factory=lambda: (
            get_env_var(AWS_LAMBDA_FUNCTION_NAME_KEY) or DEFAULT_AWS_LAMBDA_FUNCTION_NAME
        )
    )
    _function_version: str = field(
        default_factory=lambda: get_env_var(AWS_LAMBDA_FUNCTION_VERSION_KEY, default_value="1.0")
    )
    _invoked_function_arn: str = field(
        default_factory=lambda: get_env_var(AWS_LAMBDA_FUNCTION_ARN_KEY, default_value="")
    )
    _memory_limit_in_mb: int = field(
        default_factory=lambda: int(
            get_env_var(AWS_LAMBDA_FUNCTION_MEMORY_SIZE_KEY, default_value="1024")
        )
    )
    _aws_request_id: str = field(
        default_factory=lambda: get_env_var(AWS_LAMBDA_FUNCTION_REQUEST_ID_KEY, default_value="")
    )
    _log_group_name: str = field(
        default_factory=lambda: get_env_var(AWS_LAMBDA_LOG_GROUP_NAME_KEY, default_value="")
    )
    _log_stream_name: str = field(
        default_factory=lambda: get_env_var(AWS_LAMBDA_LOG_STREAM_NAME_KEY, default_value="")
    )
    _identity: LambdaCognitoIdentity = field(default_factory=lambda: LambdaCognitoIdentity())
    _client_context: LambdaClientContext = field(default_factory=lambda: LambdaClientContext())

    def __post_init__(self):
        if not self._invoked_function_arn:
            self._invoked_function_arn = (
                f"arn:aws:{get_region()}:{get_account_id()}:function:{self.function_name}"
            )
        if not self._log_group_name:
            self._log_group_name = f"/aws/lambda/{self.function_name}_docker"
        if not self._log_stream_name:
            self._log_stream_name = f"{self.aws_request_id}"


def serialize_handler(handler: LambdaHandlerType) -> str:
    """Serialize a Lambda handler to its qualified name.

    For closures (e.g., from ``LambdaHandler.get_handler()``), the function's
    ``__qualname__`` points to the closure definition site rather than the
    module-level variable that holds it. This searches ``sys.modules`` for a
    global variable that references the exact handler object, so that
    constructor arguments passed to ``get_handler()`` are preserved on
    round-trip.

    Args:
        handler (LambdaHandlerType): The Lambda handler function or class.

    Returns:
        The fully qualified module path of the handler.
    """
    # For closures, try to find a module-level variable referencing this handler
    if "<locals>" in getattr(handler, "__qualname__", ""):
        import sys

        for module_name, module in sys.modules.items():
            if module is None:
                continue
            try:
                module_dict = vars(module)
            except TypeError:
                continue
            for attr_name, attr_value in module_dict.items():
                if attr_value is handler:
                    return f"{module_name}.{attr_name}"
        # Fall back to the originating class if the variable wasn't found
        handler_class = getattr(handler, "_handler_class", None)
        if handler_class is not None:
            return get_qualified_name(handler_class)
    return get_qualified_name(handler)


def deserialize_handler(handler: str | LambdaHandlerType) -> LambdaHandlerType:
    """Deserialize a handler from its qualified name.

    Loads a handler from its fully qualified module path. Supports both
    function handlers and LambdaHandler subclasses.

    Args:
        handler (str): The fully qualified handler path (e.g., 'module.submodule.HandlerClass').

    Returns:
        The Lambda handler function ready to be invoked.

    Raises:
        ValueError: If the handler cannot be deserialized or is not a valid handler type.

    Example:
        ```python
        handler = deserialize_handler('my_module.MyHandler')
        response = handler(event, context)
        ```
    """
    if not isinstance(handler, str):
        assert callable(handler), (
            f"Invalid handler type: expected a callable or fully qualified handler path string, "
            f"got {type(handler).__name__}."
        )
        return cast(LambdaHandlerType, handler)
    handler_components = handler.split(".")

    handler_module = as_module_type(".".join(handler_components[:-1]))
    handler_name = handler_components[-1]

    handler_code = getattr(handler_module, handler_name)
    if inspect.isfunction(handler_code):
        return cast(LambdaHandlerType, handler_code)
    elif inspect.isclass(handler_code) and issubclass(handler_code, LambdaHandler):
        logger.debug(f"Handler code is a class: {handler_code}. Calling `get_handler`...")
        return handler_code.get_handler()
    else:
        raise ValueError(
            f"Unable to deserialize handler: {handler}. "
            "It is not a function or a subclass of LambdaHandler."
        )


class LambdaHandlerRequest(PydanticBaseModel):
    """Request model for dynamic Lambda handler invocation.

    Contains the handler reference and event payload for routing
    Lambda invocations to the appropriate handler.

    Attributes:
        handler: The Lambda handler function to invoke.
        event: The event payload to pass to the handler.
    """

    handler: Annotated[
        LambdaHandlerType,
        BeforeValidator(deserialize_handler),
        PlainSerializer(lambda handler: serialize_handler(handler)),
    ]
    event: LambdaEvent
