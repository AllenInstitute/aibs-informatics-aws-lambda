from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Union

from aibs_informatics_aws_utils.dynamodb import (
    ConditionBaseExpression,
    ConditionBaseExpressionString,
    DynamoDBEnvBaseTable,
    DynamoDBTable,
)
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.base import (
    CustomStringField,
    DictField,
    ListField,
    RawField,
    SchemaModel,
    StringField,
    UnionField,
    custom_field,
)
from aibs_informatics_core.models.db import (
    DynamoDBItemKey,
    DynamoDBKey,
    DynamoDBPrimaryKeyItemValue,
)
from aibs_informatics_core.utils.modules import load_type_from_qualified_name

from aibs_informatics_aws_lambda.common.handler import LambdaHandler


def primary_key_union_field() -> UnionField:
    return UnionField(
        [
            (dict, DictField()),
            (float, StringField()),
            ((str, bytes, int, float, bool), RawField()),
        ]
    )


@dataclass
class BaseDynamoDBTableRequest(SchemaModel):
    table_class_name: str

    @cached_property
    def env_base(self) -> EnvBase:
        return EnvBase.from_env()

    @cached_property
    def table_class(self) -> Union[DynamoDBTable, DynamoDBEnvBaseTable]:
        class_type = load_type_from_qualified_name(self.table_class_name)
        if issubclass(class_type, DynamoDBEnvBaseTable):
            return class_type(self.env_base)
        elif issubclass(class_type, DynamoDBTable):
            return class_type()
        else:
            raise ValueError(
                f"table class name ({self.table_class_name}) must be subclass of "
                f"DynamoDBTable or DynamoDBEnvBaseTable, not {class_type}."
            )


@dataclass
class DynamoDBTableWithKeyRequest(BaseDynamoDBTableRequest):
    key: Union[DynamoDBKey, DynamoDBPrimaryKeyItemValue] = custom_field(
        mm_field=primary_key_union_field()
    )


@dataclass
class DynamoDBTableUpdateRequest(DynamoDBTableWithKeyRequest):
    filters: List[Union[ConditionBaseExpressionString, ConditionBaseExpression]] = custom_field(
        default_factory=list,
        mm_field=ListField(
            UnionField(
                [
                    (str, CustomStringField(ConditionBaseExpressionString)),
                    (ConditionBaseExpression, ConditionBaseExpression.as_mm_field()),
                ]
            )
        ),
    )
