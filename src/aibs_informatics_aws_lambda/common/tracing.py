from typing import Optional

from aws_lambda_powertools.tracing import Tracer

from aibs_informatics_aws_lambda.common.base import HandlerMixins


class TracingMixins(HandlerMixins):
    @classmethod
    def get_tracer(cls, service: Optional[str] = None) -> Tracer:
        return Tracer(service=service)
