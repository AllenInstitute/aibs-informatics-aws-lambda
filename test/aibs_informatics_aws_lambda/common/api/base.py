from dataclasses import dataclass
from typing import List, Literal

from aibs_informatics_core.models.api.route import ApiRoute
from aibs_informatics_core.models.base import SchemaModel

from aibs_informatics_aws_lambda.common.api.handler import ApiLambdaHandler

# ------------------------------
#       Health Check
# ------------------------------


@dataclass
class HealthCheckRequest(SchemaModel):
    """Can be used as a No-op"""

    raise_exception: bool = False


@dataclass
class HealthCheckResponse(SchemaModel):
    status: Literal["OK"] = "OK"


class HealthCheckRoute(ApiRoute[HealthCheckRequest, HealthCheckResponse]):
    @classmethod
    def route_rule(cls) -> str:
        return "/health"

    @classmethod
    def route_method(cls) -> List[str]:
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


@dataclass
class GetRequest(SchemaModel):
    """Can be used as a Getter"""

    id: str


@dataclass
class GetResponse(SchemaModel):
    values: List[str]


class GetRoute(ApiRoute[GetRequest, GetResponse]):
    @classmethod
    def route_rule(cls) -> str:
        return "/get"

    @classmethod
    def route_method(cls) -> List[str]:
        return ["GET"]


class GetHandler(ApiLambdaHandler[GetRequest, GetResponse], GetRoute):
    def handle(self, request: GetRequest) -> GetResponse:
        return GetResponse(values=[request.id, f"{len(request.id)}"])
