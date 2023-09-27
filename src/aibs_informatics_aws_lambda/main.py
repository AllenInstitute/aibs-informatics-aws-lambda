import argparse
import json
import os
from typing import Optional, Sequence

from aibs_informatics_aws_utils.constants.lambda_ import (
    AWS_LAMBDA_EVENT_PAYLOAD_KEY,
    AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY,
    AWS_LAMBDA_FUNCTION_HANDLER_KEY,
    AWS_LAMBDA_FUNCTION_HANDLER_STANDARD_KEY,
)
from aibs_informatics_aws_utils.s3 import download_to_json_object, upload_json
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.utils.json import JSON, load_json_object
from aibs_informatics_core.utils.os_operations import get_env_var
from aws_lambda_powertools.utilities.typing import LambdaContext

from aibs_informatics_aws_lambda.common.handler import LambdaEvent
from aibs_informatics_aws_lambda.common.logging import get_service_logger
from aibs_informatics_aws_lambda.common.models import (
    DefaultLambdaContext,
    LambdaHandlerRequest,
    deserialize_handler,
)

logger = get_service_logger(__name__)


@logger.inject_lambda_context(log_event=True)
def handle(event: LambdaEvent, context: LambdaContext) -> Optional[JSON]:
    """Router to OTF handler function invocation

    Args:
        event (LambdaEvent): Serialized event
        context (LambdaContext): context object

    Returns:
        Optional[JSON]: response
    """
    logger.info("Parsing event")
    if not isinstance(event, dict):
        raise ValueError(f"Unable to parse event - events must be Dict type")
    request = LambdaHandlerRequest.from_dict(event)
    logger.info(f"Successfully deserialized request. Loading handler from '{request.handler}'")
    target_handler = request.handler
    logger.info(f"Successfully loaded handler. Invoking..")
    response = target_handler(request.event, context)
    logger.info(f"Invocation is complete. Returning result")
    return response


def handle_cli(args: Optional[Sequence[str]] = None):
    logger.info("Processing request from CLI")

    parser = argparse.ArgumentParser(description="CLI AWS Lambda Handler")
    parser.add_argument(
        "--handler-qualified-name",
        "--handler-name",
        "--handler",
        dest="handler_qualified_name",
        required=False,
        default=get_env_var(
            AWS_LAMBDA_FUNCTION_HANDLER_KEY, AWS_LAMBDA_FUNCTION_HANDLER_STANDARD_KEY
        ),
        help=(
            f"handler function qualified name. If not provided, will try to load from "
            f"{(AWS_LAMBDA_FUNCTION_HANDLER_KEY, AWS_LAMBDA_FUNCTION_HANDLER_STANDARD_KEY)} "
            "env variables"
        ),
    )
    parser.add_argument(
        "--payload",
        "--event",
        "-e",
        required=False,
        default=get_env_var(AWS_LAMBDA_EVENT_PAYLOAD_KEY),
        help=(
            f"event payload of function. If not provided, will try to load from "
            f"{AWS_LAMBDA_EVENT_PAYLOAD_KEY} env variable"
        ),
    )
    parser.add_argument(
        "--response-location",
        "-o",
        dest="response_location",
        required=False,
        default=get_env_var(AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY),
        help=(
            f"optional response location to store response at. can be S3 or local file. If not "
            f"provided, will load from {AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY} env variable."
        ),
    )

    logger.info("Parsing arguments")

    parsed_args = parser.parse_args(args=args)

    if parsed_args.handler_qualified_name is None:
        raise ValueError("Handler could not be resolved from argument or environment variable")
    target_handler = deserialize_handler(parsed_args.handler_qualified_name)

    if parsed_args.payload is None:
        raise ValueError("Payload could not be resolved by argument or environment variable")
    if S3URI.is_valid(parsed_args.payload):
        logger.info("Payload is an S3 path. downloading to JSON")
        event = download_to_json_object(S3URI(parsed_args.payload))
    else:
        event = load_json_object(parsed_args.payload)

    logger.info(f"Successfully loaded handler. Invoking..")
    response = target_handler(event, DefaultLambdaContext())
    logger.info(f"Invocation complete.")

    response_location = parsed_args.response_location
    if response_location:
        response = response or {}
        logger.info(f"Response location specified: {response_location}")
        if S3URI.is_valid(response_location):
            logger.info(f"Uploading result to S3")
            upload_json(response, S3URI(response_location))
        elif not os.path.isdir(response_location) and os.access(
            os.path.dirname(response_location), os.W_OK
        ):
            logger.info(f"Writing result locally")
            with open(response_location, "w") as f:
                json.dump(response, f, sort_keys=True)
        else:
            raise ValueError(
                f"Response location specified {response_location}, but not a valid s3/local path"
            )
    else:
        logger.info(f"Response location NOT specified. Response not saved.")

    logger.info("Handler execution complete.")


if __name__ == "__main__":
    handle_cli()
