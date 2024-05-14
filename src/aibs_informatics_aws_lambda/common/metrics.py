from datetime import datetime
from typing import Optional, Union

from aws_lambda_powertools.metrics import EphemeralMetrics, Metrics, MetricUnit

from aibs_informatics_aws_lambda.common.base import HandlerMixins

METRICS_ATTR = "_metrics"

DEFAULT_TIME_START = datetime.now()


def add_duration_metric(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    name: str = "",
    metrics: Optional[Union[EphemeralMetrics, Metrics]] = None,
):
    start = start or DEFAULT_TIME_START
    end = end or datetime.now(start.tzinfo)
    duration = end - start
    if metrics is None:
        metrics = EphemeralMetrics()
    metrics.add_metric(
        name=f"{name}Duration", unit=MetricUnit.Milliseconds, value=duration.total_seconds() * 1000
    )


def add_success_metric(name: str = "", metrics: Optional[Union[EphemeralMetrics, Metrics]] = None):
    if metrics is None:
        metrics = EphemeralMetrics()
    metrics.add_metric(name=f"{name}Success", unit=MetricUnit.Count, value=1)
    metrics.add_metric(name=f"{name}Failure", unit=MetricUnit.Count, value=0)


def add_failure_metric(name: str = "", metrics: Optional[Union[EphemeralMetrics, Metrics]] = None):
    if metrics is None:
        metrics = EphemeralMetrics()
    metrics.add_metric(name=f"{name}Success", unit=MetricUnit.Count, value=0)
    metrics.add_metric(name=f"{name}Failure", unit=MetricUnit.Count, value=1)


class EnhancedMetrics(Metrics):
    def add_count_metric(self, name: str, value: float):
        self.add_metric(name=name, unit=MetricUnit.Count, value=value)

    def add_duration_metric(
        self, start: Optional[datetime] = None, end: Optional[datetime] = None, name: str = ""
    ):
        add_duration_metric(start=start, end=end, name=name, metrics=self)

    def add_success_metric(self, name: str = ""):
        add_success_metric(name=name, metrics=self)

    def add_failure_metric(self, name: str = ""):
        add_failure_metric(name=name, metrics=self)


class MetricsMixins(HandlerMixins):
    @property
    def metrics(self) -> EnhancedMetrics:
        try:
            return self._metrics
        except AttributeError:
            self.metrics = self.get_metrics(handler_name=self.handler_name())
        return self.metrics

    @metrics.setter
    def metrics(self, value: EnhancedMetrics):
        self._metrics = value

    @classmethod
    def get_metrics(
        cls,
        service: Optional[str] = None,
        namespace: Optional[str] = None,
        **additional_dimensions: str,
    ) -> EnhancedMetrics:
        metrics = EnhancedMetrics(service=service, namespace=namespace)
        for dimension_name, dimension_value in additional_dimensions.items():
            metrics.add_dimension(name=dimension_name, value=dimension_value)
        return metrics

    @classmethod
    def add_metric(
        cls,
        metrics: Metrics,
        name: str,
        value: float,
        unit: MetricUnit = MetricUnit.Count,
    ):
        metrics.add_metric(name=name, unit=unit, value=value)
