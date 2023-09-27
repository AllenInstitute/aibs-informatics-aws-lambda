import os
from unittest import mock

import moto
import pytest


@pytest.fixture(scope="function")
def aws_credentials_fixture():
    """Set testing credentials for mocked AWS resources and
    avoid accidentally hitting anything live with boto3.
    """
    # Clear os.environ dict (will be restored after fixture is finished)
    with mock.patch.dict(os.environ, clear=True):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
        os.environ["AWS_REGION"] = "us-west-2"
        yield
