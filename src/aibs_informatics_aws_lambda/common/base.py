from aws_lambda_powertools.utilities.typing import LambdaContext

CONTEXT_ATTR = "_context"


class HandlerMixins:
    @property
    def context(self) -> LambdaContext:
        if not hasattr(self, CONTEXT_ATTR):
            raise ValueError(f"{self.__class__.__name__}")
        return getattr(self, CONTEXT_ATTR)

    @context.setter
    def context(self, value: LambdaContext):
        setattr(self, CONTEXT_ATTR, value)

    @classmethod
    def handler_name(cls) -> str:
        return cls.__name__

    @classmethod
    def service_name(cls) -> str:
        return cls.__name__
