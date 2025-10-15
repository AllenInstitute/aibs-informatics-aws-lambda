from aibs_informatics_core.models.email_address import EmailAddress
from pytest import mark, param

from aibs_informatics_aws_lambda.handlers.notifications.notifiers.model import (
    NotificationContent,
    SESEmailTarget,
)
from test.base import does_not_raise

ADDR1 = "email1@fake_address.com"
ADDR2 = "email2@fake_address.com"
ADDR3 = "email3@fake_address.com"
ADDR4 = "email4@fake_address.com"


@mark.parametrize(
    "value,expected,raise_expectation",
    [
        param(
            {"recipients": [ADDR1]},
            SESEmailTarget(recipients=[EmailAddress(ADDR1)]),
            does_not_raise(),
            id="simple",
        ),
        param(
            {"recipients": [ADDR1, ADDR1], "recipient": ADDR2},
            SESEmailTarget(recipients=[EmailAddress(ADDR1), EmailAddress(ADDR2)]),
            does_not_raise(),
            id="captures duplicates",
        ),
        param(
            {"recipients": ADDR1, "recipient": [ADDR2], "addresses": [ADDR3]},
            SESEmailTarget(
                recipients=[EmailAddress(ADDR1), EmailAddress(ADDR2), EmailAddress(ADDR3)]
            ),
            does_not_raise(),
            id="handles aliases",
        ),
    ],
)
def test__SESEmailTarget__from_dict(value, expected, raise_expectation):
    with raise_expectation:
        actual = SESEmailTarget.from_dict(value)

    if expected:
        assert actual == expected


@mark.parametrize(
    "value,expected,raise_expectation",
    [
        param(
            {"subject": "subject", "message": "message"},
            NotificationContent(subject="subject", message="message"),
            does_not_raise(),
            id="simple",
        ),
        param(
            {"subject": "subject", "body": "message", "content": "content"},
            NotificationContent(subject="subject", message="message"),
            does_not_raise(),
            id="handles aliases",
        ),
    ],
)
def test__NotificationContent__from_dict(value, expected, raise_expectation):
    with raise_expectation:
        actual = NotificationContent.from_dict(value)

    if expected:
        assert actual == expected
