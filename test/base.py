import os
from contextlib import nullcontext as does_not_raise
from typing import Optional

from aibs_informatics_core.env import ENV_BASE_KEY, EnvBase, EnvType
from aibs_informatics_test_resources import BaseTest as _BaseTest


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
