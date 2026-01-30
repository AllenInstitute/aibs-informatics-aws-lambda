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
    """Base class for creating strongly-typed AWS Lambda handlers.

    Provides a foundation for Lambda functions with built-in support for:
    - Request/response serialization and deserialization
    - Structured logging via AWS Lambda Powertools
    - CloudWatch metrics collection
    - SQS batch processing
    - DynamoDB Streams processing

    Inherit from the LambdaHandler class to create a custom strongly typed lambda handler
    that expects a REQUEST object and returns a RESPONSE object that follow the `ModelProtocol`.

    Type Parameters:
        REQUEST: The request model type (must implement ModelProtocol).
        RESPONSE: The response model type (must implement ModelProtocol).

    Example:
        ```python
        @dataclass
        class MyRequest(SchemaModel):
            name: str

        @dataclass
        class MyResponse(SchemaModel):
            message: str

        class MyHandler(LambdaHandler[MyRequest, MyResponse]):
            def handle(self, request: MyRequest) -> MyResponse:
                return MyResponse(message=f"Hello, {request.name}!")

        handler = MyHandler.get_handler()
        ```
    """

    def __post_init__(self):
        self.context = LambdaContext()
        super().__post_init__()

    @classmethod
    def load_input__remote(cls, remote_path: S3URI) -> JSON:
        """Load input data from a remote S3 location.

        Args:
            remote_path (S3URI): The S3 URI to download the input from.

        Returns:
            The JSON content from the S3 object.
        """
        return download_to_json_object(remote_path)

    @classmethod
    def write_output__remote(cls, output: JSON, remote_path: S3URI) -> None:
        """Write output data to a remote S3 location.

        Args:
            output (JSON): The JSON content to upload.
            remote_path (S3URI): The S3 URI to upload the output to.
        """
        return upload_json(output, remote_path)

    # --------------------------------------------------------------------
    # Handler provider methods
    # --------------------------------------------------------------------

    @classmethod
    def get_handler(cls, *args, **kwargs) -> LambdaHandlerType:
        """Create a Lambda handler function for this handler class.

        Creates a wrapped handler function that:
        - Injects Lambda context for logging
        - Instantiates the handler class
        - Deserializes the incoming event
        - Invokes the handle method
        - Serializes and returns the response

        Args:
            *args: Positional arguments passed to the handler constructor.
            **kwargs: Keyword arguments passed to the handler constructor.

        Returns:
            A callable Lambda handler function suitable for AWS Lambda.

        Example:
            ```python
            # In your Lambda module
            handler = MyHandler.get_handler()
            ```
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
        """Create a handler for processing SQS batch records.

        Creates a Lambda handler that processes batches of SQS messages
        with partial failure support, allowing successful messages to be
        acknowledged even if some fail.

        See Also:
            https://docs.powertools.aws.dev/lambda/python/latest/utilities/batch/

        Args:
            *args: Positional arguments passed to the handler constructor.
            queue_type (Literal["standard", "fifo"]): The SQS queue type - "standard" or "fifo".
                Defaults to "standard".
            **kwargs: Keyword arguments passed to the handler constructor.

        Returns:
            A callable Lambda handler function for SQS batch processing.

        Raises:
            RuntimeError: If an invalid queue_type is provided.

        Example:
            ```python
            handler = MyHandler.get_sqs_batch_handler(queue_type="fifo")
            ```
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
        """Create a handler for processing DynamoDB Stream events.

        Creates a Lambda handler that processes batches of DynamoDB Stream
        records with partial failure support.

        See Also:
            https://docs.powertools.aws.dev/lambda/python/latest/utilities/batch/

        Args:
            *args: Positional arguments passed to the handler constructor.
            **kwargs: Keyword arguments passed to the handler constructor.

        Returns:
            A callable Lambda handler function for DynamoDB Streams.

        Example:
            ```python
            handler = MyHandler.get_dynamodb_stream_handler()
            ```
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
        return (
            f"{self.__class__.__name__}("
            f"request: {self.get_request_cls()}, "
            f"response: {self.get_response_cls()}"
            ")"
        )
