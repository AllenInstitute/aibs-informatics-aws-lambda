__all__ = [
    "BaseTest",
    "AwsBaseTest",
    "does_not_raise",
]

from contextlib import nullcontext as does_not_raise
from typing import Optional

from aibs_informatics_core.env import ENV_BASE_KEY, EnvBase, EnvType
from aibs_informatics_test_resources import BaseTest as _BaseTest
from botocore.client import BaseClient
from botocore.stub import Stubber


class BaseTest(_BaseTest):
    maxDiff: Optional[int] = None

    @property
    def env_base(self) -> EnvBase:
        if not hasattr(self, "_env_base"):
            self._env_base = EnvBase.from_type_and_label(EnvType.DEV, "marmotdev")
        return self._env_base

    @env_base.setter
    def env_base(self, env_base: EnvBase):
        self._env_base = env_base

    def set_env_base_env_var(self, env_base: Optional[EnvBase] = None):
        self.set_env_vars((ENV_BASE_KEY, env_base or self.env_base))

    def set_aws_credentials(self):
        self.set_env_vars(
            ("AWS_ACCESS_KEY_ID", "testing"),
            ("AWS_SECRET_ACCESS_KEY", "testing"),
            ("AWS_SECURITY_TOKEN", "testing"),
            ("AWS_SESSION_TOKEN", "testing"),
            ("AWS_DEFAULT_REGION", "us-west-2"),
            ("AWS_REGION", "us-west-2"),
            ("ACCOUNT", "123456789012"),
        )


class AwsBaseTest(BaseTest):
    ACCOUNT_ID = "123456789012"
    US_EAST_1 = "us-east-1"
    US_WEST_2 = "us-west-2"

    @property
    def DEFAULT_REGION(self) -> str:
        return self.US_WEST_2

    @property
    def DEFAULT_SECRET_KEY(self) -> str:
        return "A" * 20

    @property
    def DEFAULT_ACCESS_KEY(self) -> str:
        return "A" * 20

    def set_region(self, region: Optional[str] = None):
        self.set_env_vars(
            ("AWS_REGION", region or self.DEFAULT_REGION),
            ("AWS_DEFAULT_REGION", region or self.DEFAULT_REGION),
        )

    def set_credentials(self, access_key: Optional[str] = None, secret_key: Optional[str] = None):
        self.set_env_vars(
            ("AWS_ACCESS_KEY_ID", access_key or self.DEFAULT_ACCESS_KEY),
            ("AWS_SECRET_ACCESS_KEY", secret_key or self.DEFAULT_SECRET_KEY),
            ("AWS_SECURITY_TOKEN", "testing"),
            ("AWS_SESSION_TOKEN", "testing"),
        )

    def set_account_id(self, account_id: Optional[str] = None):
        self.set_env_vars(("AWS_ACCOUNT_ID", account_id or self.ACCOUNT_ID))

    def set_aws_credentials(self):
        self.set_credentials(access_key="testing", secret_key="testing")
        self.set_region()
        self.set_account_id()

    def stub(self, client: BaseClient) -> Stubber:
        return Stubber(client=client)
