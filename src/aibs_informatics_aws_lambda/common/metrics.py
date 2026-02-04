"""Metrics utilities for AWS Lambda handlers.

Provides utilities for collecting and publishing CloudWatch metrics
using AWS Lambda Powertools.
"""

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
    """Add a duration metric to the metrics collector.

    Calculates the duration between start and end times and records
    it as a CloudWatch metric in milliseconds.

    Args:
        start (Optional[datetime]): The start timestamp. Defaults to module load time.
        end (Optional[datetime]): The end timestamp. Defaults to current time.
        name (str): Prefix for the metric name. Final name is '{name}Duration'.
        metrics (Optional[Union[EphemeralMetrics, Metrics]]): The metrics collector to use.
            Creates ephemeral if None.
    """
    start = start or DEFAULT_TIME_START
    end = end or datetime.now(start.tzinfo)
    duration = end - start
    if metrics is None:
        metrics = EphemeralMetrics()
    metrics.add_metric(
        name=f"{name}Duration", unit=MetricUnit.Milliseconds, value=duration.total_seconds() * 1000
    )


def add_success_metric(name: str = "", metrics: Optional[Union[EphemeralMetrics, Metrics]] = None):
    """Record a successful operation metric.

    Adds metrics indicating success (1) and failure (0) counts.

    Args:
        name (str): Prefix for the metric names.
        metrics (Optional[Union[EphemeralMetrics, Metrics]]): The metrics collector to use.
            Creates ephemeral if None.
    """
    if metrics is None:
        metrics = EphemeralMetrics()
    metrics.add_metric(name=f"{name}Success", unit=MetricUnit.Count, value=1)
    metrics.add_metric(name=f"{name}Failure", unit=MetricUnit.Count, value=0)


def add_failure_metric(name: str = "", metrics: Optional[Union[EphemeralMetrics, Metrics]] = None):
    """Record a failed operation metric.

    Adds metrics indicating success (0) and failure (1) counts.

    Args:
        name (str): Prefix for the metric names.
        metrics (Optional[Union[EphemeralMetrics, Metrics]]): The metrics collector to use.
            Creates ephemeral if None.
    """
    if metrics is None:
        metrics = EphemeralMetrics()
    metrics.add_metric(name=f"{name}Success", unit=MetricUnit.Count, value=0)
    metrics.add_metric(name=f"{name}Failure", unit=MetricUnit.Count, value=1)


class EnhancedMetrics(Metrics):
    """Extended Metrics class with convenience methods.

    Provides additional helper methods for common metric patterns
    like counting, duration tracking, and success/failure recording.
    """

    def add_count_metric(self, name: str, value: float):
        """Add a count metric.

        Args:
            name (str): The metric name.
            value (float): The count value.
        """
        self.add_metric(name=name, unit=MetricUnit.Count, value=value)

    def add_duration_metric(
        self, start: Optional[datetime] = None, end: Optional[datetime] = None, name: str = ""
    ):
        """Add a duration metric.

        Args:
            start (Optional[datetime]): The start timestamp. Defaults to module load time.
            end (Optional[datetime]): The end timestamp. Defaults to current time.
            name (str): Prefix for the metric name.
        """
        add_duration_metric(start=start, end=end, name=name, metrics=self)

    def add_success_metric(self, name: str = ""):
        """Record a successful operation metric.

        Args:
            name (str): Prefix for the metric names.
        """
        add_success_metric(name=name, metrics=self)

    def add_failure_metric(self, name: str = ""):
        """Record a failed operation metric.

        Args:
            name (str): Prefix for the metric names.
        """
        add_failure_metric(name=name, metrics=self)


class MetricsMixins(HandlerMixins):
    """Mixin class providing CloudWatch metrics capabilities.

    Integrates AWS Lambda Powertools Metrics for automatic
    metric collection and publishing to CloudWatch.

    """

    @property
    def metrics(self) -> EnhancedMetrics:
        """Get the metrics collector, creating one if needed.

        Returns:
            The EnhancedMetrics instance for this handler.
        """
        try:
            return self._metrics
        except AttributeError:
            self.metrics = self.get_metrics(handler_name=self.handler_name())
        return self.metrics

    @metrics.setter
    def metrics(self, value: EnhancedMetrics):
        """Set the metrics collector.

        Args:
            value (EnhancedMetrics): The EnhancedMetrics instance to set.
        """
        self._metrics = value

    @classmethod
    def get_metrics(
        cls,
        service: Optional[str] = None,
        namespace: Optional[str] = None,
        **additional_dimensions: str,
    ) -> EnhancedMetrics:
        """Create a new EnhancedMetrics instance.

        Args:
            service (Optional[str]): The service name for metrics.
            namespace (Optional[str]): The CloudWatch namespace.
            **additional_dimensions (str): Additional metric dimensions as key-value pairs.

        Returns:
            A configured EnhancedMetrics instance.
        """
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
        """Add a metric to the collector.

        Args:
            metrics (Metrics): The metrics collector.
            name (str): The metric name.
            value (float): The metric value.
            unit (MetricUnit): The metric unit. Defaults to Count.
        """
        metrics.add_metric(name=name, unit=unit, value=value)
