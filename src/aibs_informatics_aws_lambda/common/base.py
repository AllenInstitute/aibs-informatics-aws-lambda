from aws_lambda_powertools.utilities.typing import LambdaContext

CONTEXT_ATTR = "_context"


class HandlerMixins:
    """Mixin class providing common handler utilities.

    Provides access to the Lambda context and handler/service name utilities
    that are shared across all Lambda handlers.

    Attributes:
        context: The AWS Lambda context object for the current invocation.
    """

    @property
    def context(self) -> LambdaContext:
        """Get the Lambda context for the current invocation.

        Returns:
            The AWS Lambda context object.

        Raises:
            ValueError: If context has not been set.
        """
        if not hasattr(self, CONTEXT_ATTR):
            raise ValueError(f"{self.__class__.__name__}")
        return getattr(self, CONTEXT_ATTR)

    @context.setter
    def context(self, value: LambdaContext):
        """Set the Lambda context for the current invocation.

        Args:
            value: The AWS Lambda context object to set.
        """
        setattr(self, CONTEXT_ATTR, value)

    @classmethod
    def handler_name(cls) -> str:
        """Get the name of this handler class.

        Returns:
            The class name as a string.
        """
        return cls.__name__

    @classmethod
    def service_name(cls) -> str:
        """Get the service name for logging and metrics.

        Returns:
            The class name as the service identifier.
        """
        return cls.__name__
