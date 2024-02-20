from test.aibs_informatics_aws_lambda.common.test_handler import CounterHandler_ReqResp
from test.base import does_not_raise
from typing import Optional

from aibs_informatics_core.utils.json import JSON
from aws_lambda_powertools.utilities.typing import LambdaContext
from pytest import mark, param, raises

from aibs_informatics_aws_lambda.common.handler import LambdaEvent
from aibs_informatics_aws_lambda.common.models import LambdaHandlerRequest, deserialize_handler


def mock_handler(event: LambdaEvent, context: LambdaContext) -> Optional[JSON]:
    if isinstance(event, dict):
        if event.get("fail", False):
            raise ValueError("something went wrong")
        if event.get("response", False):
            return event
    return None


DUMMY_VARIABLE = 1


@mark.parametrize(
    "value,expected,raise_expectation",
    [
        param(
            {
                "handler": "test.aibs_informatics_aws_lambda.common.test_models.mock_handler",
                "event": {},
            },
            LambdaHandlerRequest(mock_handler, {}),
            does_not_raise(),
            id="simple",
        ),
    ],
)
def test__get_qualified_name(value, expected: Optional[LambdaHandlerRequest], raise_expectation):
    with raise_expectation:
        actual = LambdaHandlerRequest.from_dict(value)

    if expected:
        assert actual.handler.__name__ == expected.handler.__name__
        assert actual.event == expected.event


def test__deserialize_handler__handles_function():
    expected = mock_handler
    actual = deserialize_handler(
        "test.aibs_informatics_aws_lambda.common.test_models.mock_handler"
    )
    assert actual.__name__ == expected.__name__
    assert actual.__module__ == expected.__module__
    assert actual == expected


def test__deserialize_handler__handles_class():
    expected = CounterHandler_ReqResp.get_handler()
    actual = deserialize_handler(
        "test.aibs_informatics_aws_lambda.common.test_handler.CounterHandler_ReqResp"
    )
    assert actual.__name__ == expected.__name__
    assert actual.__module__ == expected.__module__


def test__deserialize_handler__handles_class_instance():
    with raises(ValueError):
        deserialize_handler("test.aibs_informatics_aws_lambda.common.test_models.DUMMY_VARIABLE")
