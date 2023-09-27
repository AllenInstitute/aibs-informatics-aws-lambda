from typing import Optional

from aws_lambda_powertools.tracing import Tracer

from aibs_informatics_aws_lambda.common.base import HandlerMixins


class TracingMixins(HandlerMixins):
    @property
    def tracer(self) -> Tracer:
        try:
            return self._tracer
        except AttributeError:
            self.tracer = self.get_tracer(self.service_name())
        return self.tracer

    @tracer.setter
    def tracer(self, value: Tracer):
        self._tracer = value

    @classmethod
    def get_tracer(cls, service: Optional[str] = None) -> Tracer:
        return Tracer(service=service)
