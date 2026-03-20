from aibs_informatics_core.utils.json import JSON
from aws_lambda_powertools.utilities.typing import LambdaContext
from pytest import mark, param, raises

from aibs_informatics_aws_lambda.common.handler import LambdaEvent
from aibs_informatics_aws_lambda.common.models import LambdaHandlerRequest, deserialize_handler
from test.aibs_informatics_aws_lambda.common.test_handler import CounterHandler_ReqResp
from test.base import does_not_raise


def mock_handler(event: LambdaEvent, context: LambdaContext) -> JSON | None:
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
            LambdaHandlerRequest(handler=mock_handler, event={}),
            does_not_raise(),
            id="simple",
        ),
    ],
)
def test__get_qualified_name(value, expected: LambdaHandlerRequest | None, raise_expectation):
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


def test__deserialize_handler__handles_callable_passthrough():
    actual = deserialize_handler(mock_handler)
    assert actual is mock_handler


def test__deserialize_handler__raises_for_non_callable():
    with raises(AssertionError, match="expected a callable"):
        deserialize_handler(42)  # type: ignore[arg-type]


# --- PlainSerializer tests (to_dict / serialization) ---


def test__serialize_handler__function_to_qualified_name():
    request = LambdaHandlerRequest(handler=mock_handler, event={})
    result = request.to_dict()
    assert result["handler"] == "test.aibs_informatics_aws_lambda.common.test_models.mock_handler"


counter_handler = CounterHandler_ReqResp.get_handler()


def test__serialize_handler__class_handler_to_qualified_name():
    request = LambdaHandlerRequest(handler=counter_handler, event={})
    result = request.to_dict()
    # The serialized handler should resolve to the module-level variable
    assert (
        result["handler"]
        == "test.aibs_informatics_aws_lambda.common.test_models.counter_handler"
    )


def test__serialize_handler__preserves_event():
    event_data = {"key": "value", "nested": {"a": 1}}
    request = LambdaHandlerRequest(handler=mock_handler, event=event_data)
    result = request.to_dict()
    assert result["event"] == event_data


def test__serialize_handler__roundtrip_function():
    """Deserialize from dict, serialize back, and verify consistency."""
    original = {
        "handler": "test.aibs_informatics_aws_lambda.common.test_models.mock_handler",
        "event": {"foo": "bar"},
    }
    request = LambdaHandlerRequest.from_dict(original)
    serialized = request.to_dict()
    assert serialized["handler"] == original["handler"]
    assert serialized["event"] == original["event"]

    # Deserialize again from the serialized output
    request2 = LambdaHandlerRequest.from_dict(serialized)
    assert request2.handler.__name__ == request.handler.__name__
    assert request2.event == request.event


def test__serialize_handler__roundtrip_class():
    """Deserialize a class handler from dict, serialize back, and verify consistency."""
    original = {
        "handler": "test.aibs_informatics_aws_lambda.common.test_handler.CounterHandler_ReqResp",
        "event": {"count": 5},
    }
    request = LambdaHandlerRequest.from_dict(original)
    serialized = request.to_dict()
    assert isinstance(serialized["handler"], str)
    assert serialized["event"] == original["event"]

    # Roundtrip: re-deserialize should produce equivalent handler
    request2 = LambdaHandlerRequest.from_dict(serialized)
    assert request2.handler.__name__ == request.handler.__name__
    assert request2.event == request.event
