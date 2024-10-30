import json
import logging
from dataclasses import dataclass
from typing import Callable, Generic, Literal, Optional, TypeVar, Union, cast

from aibs_informatics_aws_utils.s3 import download_to_json_object, upload_json
from aibs_informatics_core.executors.base import BaseExecutor
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.models.base import ModelProtocol
from aibs_informatics_core.utils.json import JSON
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    SqsFifoPartialProcessor,
    batch_processor,
    process_partial_response,
)
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import DynamoDBRecord
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext

from aibs_informatics_aws_lambda.common.base import HandlerMixins
from aibs_informatics_aws_lambda.common.logging import LoggingMixins
from aibs_informatics_aws_lambda.common.metrics import MetricsMixins

LambdaEvent = Union[JSON]  # type: ignore # https://github.com/python/mypy/issues/7866
LambdaHandlerType = Callable[[LambdaEvent, LambdaContext], Optional[JSON]]
logger = logging.getLogger(__name__)

REQUEST = TypeVar("REQUEST", bound=ModelProtocol)
RESPONSE = TypeVar("RESPONSE", bound=ModelProtocol)


@dataclass  # type: ignore[misc] # mypy #5374
class LambdaHandler(
    LoggingMixins,
    MetricsMixins,
    HandlerMixins,
    BaseExecutor[REQUEST, RESPONSE],
    Generic[REQUEST, RESPONSE],
):
    def __post_init__(self):
        self.context = LambdaContext()
        super().__post_init__()

    @classmethod
    def load_input__remote(cls, remote_path: S3URI) -> JSON:
        return download_to_json_object(remote_path)

    @classmethod
    def write_output__remote(cls, output: JSON, remote_path: S3URI) -> None:
        return upload_json(output, remote_path)

    # --------------------------------------------------------------------
    # Handler provider methods
    # --------------------------------------------------------------------

    @classmethod
    def get_handler(cls, *args, **kwargs) -> LambdaHandlerType:
        """Get a Lambda handler function for given handler class.

        Returns:
            Callable[[LambdaEvent, LambdaContext], Optional[JSON]]: Lambda handler function
        """

        logger = cls.get_logger(service=cls.service_name(), add_to_root=False)

        @logger.inject_lambda_context(log_event=True)
        def handler(event: LambdaEvent, context: LambdaContext) -> Optional[JSON]:
            lambda_handler = cls(*args, **kwargs)  # type: ignore[call-arg]
            logger.info(f"Instantiated {lambda_handler}.")
            lambda_handler.log = logger
            lambda_handler.context = context
            lambda_handler.add_logger_to_root()

            lambda_handler.log.info(f"Deserializing event: {event}")

            request = lambda_handler.deserialize_request(event)

            lambda_handler.log.info("Event successfully deserialized. Calling handler...")
            response = lambda_handler.handle(request=request)

            lambda_handler.log.info(
                f"Handler completed and returned following response: {response}"
            )
            if response:
                lambda_handler.log.info("Serializing response")
                return lambda_handler.serialize_response(response)

            return None

        return handler

    @classmethod
    def should_process_sqs_record(cls, record: SQSRecord) -> bool:
        """Filter for whether to handle an SQS Record.

        This is invoked prior to deserializing and handling that SQS message.

        Args:
            record (SQSRecord): An SQS record

        Returns:
            bool: True if handler should process request
        """
        return True

    @classmethod
    def deserialize_sqs_record(cls, record: SQSRecord) -> REQUEST:
        """Deserialize an SQS Record into the Request object of this handler

        By default, the "body" of the SQS record is deserialized using
        the class default `deserialize_request` method.

        Args:
            record (SQSRecord): An SQS record

        Returns:
            REQUEST: The expected Request object for this handler class
        """
        return cls.deserialize_request(json.loads(record["body"]))

    @classmethod
    def get_sqs_batch_handler(
        cls, *args, queue_type: Literal["standard", "fifo"] = "standard", **kwargs
    ) -> LambdaHandlerType:
        """Creates a handler for processing SQS batch records

        More info:
        https://docs.powertools.aws.dev/lambda/python/2.26.1/utilities/batch/#processing-messages-from-sqs

        Returns:
            Callable[[LambdaEvent, LambdaContext], Optional[JSON]]: [description]
        """
        if queue_type == "standard":
            processor = BatchProcessor(event_type=EventType.SQS)
        elif queue_type == "fifo":
            processor = SqsFifoPartialProcessor()
        else:
            raise RuntimeError(
                "An invalid SQS queue_type ({queue_type}) was provided to the "
                "get_sqs_batch_handler() method. Valid values include: "
                "[standard, fifo]"
            )
        logger = cls.get_logger(cls.service_name())

        # Create a record handler for each record in batch.
        def record_handler(record: SQSRecord) -> Optional[JSON]:
            if not cls.should_process_sqs_record(record):
                logger.info(f"SQS record {record} elected not to be processed.")
                return None
            lambda_handler = cls(*args, **kwargs)
            lambda_handler.log = logger
            lambda_handler.add_logger_to_root()

            request = lambda_handler.deserialize_sqs_record(record)
            response = lambda_handler.handle(request=request)
            if response:
                lambda_handler.log.info("Sending Response")
                return lambda_handler.serialize_response(response)
            lambda_handler.log.info("Not sending Response")
            return None

        # Now create top-level handler
        @logger.inject_lambda_context(log_event=True)
        def handler(event: dict, context: LambdaContext) -> PartialItemFailureResponse:
            return process_partial_response(
                event=event,
                record_handler=record_handler,
                processor=processor,
                context=context,
            )

        return cast(LambdaHandlerType, handler)

    @classmethod
    def should_process_dynamodb_record(cls, record: DynamoDBRecord) -> bool:
        """Filter for whether to handle an DynamoDB record

        This allows to filter all stream events based on:
            1. The type of event (entry modification, insertion, deletion...)
            2. The content of the affected record.

        Args:
            record (DynamoDBRecord): A DynamoDB record generated from a DynamoDB Stream

        Returns:
            bool: True if handler should process request
        """
        return True

    @classmethod
    def deserialize_dynamodb_record(cls, record: DynamoDBRecord) -> REQUEST:
        """Parse a DynamoDB record into a request object.

        This should be implemented if expected to process a dynamo DB record

        Args:
            record (DynamoDBRecord): A DynamoDB record generated from a DynamoDB Stream

        Returns:
            REQUEST: Expected Request object
        """
        raise NotImplementedError(  # pragma: no cover
            "You must implement this method if processing dynamoDB stream events"
        )

    @classmethod
    def get_dynamodb_stream_handler(cls, *args, **kwargs) -> LambdaHandlerType:
        """Creates a handler for processing DynamoDB stream events
        More info:
        https://awslabs.github.io/aws-lambda-powertools-python/1.25.1/utilities/batch/#processing-messages-from-dynamodb

        Returns:
            Callable[[LambdaEvent, LambdaContext], Optional[JSON]]: [description]
        """
        processor = BatchProcessor(event_type=EventType.DynamoDBStreams)
        logger = cls.get_logger(cls.service_name())

        # Create a record handler for each record in batch.
        def record_handler(record: DynamoDBRecord) -> Optional[JSON]:
            if not cls.should_process_dynamodb_record(record):
                logger.info(f"DynamoDB record {record} will not be processed.")
                return None
            lambda_handler = cls(*args, **kwargs)  # type: ignore[call-arg]
            lambda_handler.log = logger
            lambda_handler.add_logger_to_root()

            request = lambda_handler.deserialize_dynamodb_record(record)
            response = lambda_handler.handle(request=request)
            if response:
                lambda_handler.log.info("Sending Response")
                return lambda_handler.serialize_response(response)
            lambda_handler.log.info("Not sending Response")
            return None

        # Now create top-level handler
        @logger.inject_lambda_context(log_event=True)
        @batch_processor(record_handler=record_handler, processor=processor)  # type: ignore
        def handler(event, context: LambdaContext):
            return processor.response()

        return handler  # type: ignore

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(request: {self.get_request_cls()}, response: {self.get_response_cls()})"
