from typing import Literal

from aibs_informatics_core.models.api.route import ApiRoute
from aibs_informatics_core.models.base import PydanticBaseModel

from aibs_informatics_aws_lambda.common.api.handler import ApiLambdaHandler

# ------------------------------
#       Health Check
# ------------------------------


class HealthCheckRequest(PydanticBaseModel):
    """Can be used as a No-op"""

    raise_exception: bool = False


class HealthCheckResponse(PydanticBaseModel):
    status: Literal["OK"] = "OK"


class HealthCheckRoute(ApiRoute[HealthCheckRequest, HealthCheckResponse]):
    @classmethod
    def route_rule(cls) -> str:
        return "/health"

    @classmethod
    def route_method(cls) -> list[str]:
        return ["GET"]


class HealthCheckHandler(
    ApiLambdaHandler[HealthCheckRequest, HealthCheckResponse], HealthCheckRoute
):
    def handle(self, request: HealthCheckRequest) -> HealthCheckResponse:
        if request.raise_exception:
            raise ValueError("This is an exception")
        return HealthCheckResponse(status="OK")


# ------------------------------
#           Get
# ------------------------------


class GetRequest(PydanticBaseModel):
    """Can be used as a Getter"""

    id: str


class GetResponse(PydanticBaseModel):
    values: list[str]


class GetRoute(ApiRoute[GetRequest, GetResponse]):
    @classmethod
    def route_rule(cls) -> str:
        return "/get"

    @classmethod
    def route_method(cls) -> list[str]:
        return ["GET"]


class GetHandler(ApiLambdaHandler[GetRequest, GetResponse], GetRoute):
    def handle(self, request: GetRequest) -> GetResponse:
        return GetResponse(values=[request.id, f"{len(request.id)}"])
