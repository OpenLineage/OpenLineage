# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING, ClassVar, List, Optional

from openlineage.client.utils import RedactMixin

if TYPE_CHECKING:
    from openlineage.common.sql import DbTableMeta


class DbColumn(RedactMixin):
    _skip_redact: ClassVar[List[str]] = ["name", "type", "ordinal_position"]

    def __init__(
        self,
        name: str,
        type: str,
        description: Optional[str] = None,
        ordinal_position: Optional[int] = None,
    ):
        self.name = name
        self.type = type
        self.description = description
        self.ordinal_position = ordinal_position

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.type == other.type
            and self.description == other.description
            and self.ordinal_position == other.ordinal_position
        )

    def __repr__(self):
        return f"DbColumn({self.name!r},{self.type!r}, \
                          {self.description!r},{self.ordinal_position!r})"


class DbTableSchema(RedactMixin):
    _skip_redact: ClassVar[List[str]] = ["schema_name", "table_name"]

    def __init__(
        self,
        schema_name: str,
        table_name: "DbTableMeta",
        columns: List[DbColumn],
    ):
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns = columns

    def __eq__(self, other):
        return (
            self.schema_name == other.schema_name
            and self.table_name == other.table_name
            and self.columns == other.columns
        )

    def __repr__(self):
        return f"DbTableSchema({self.schema_name!r},{self.table_name!r}, \
                               {self.columns!r})"
