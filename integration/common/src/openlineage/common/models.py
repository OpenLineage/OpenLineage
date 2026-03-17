# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING, ClassVar

from openlineage.client.utils import RedactMixin

if TYPE_CHECKING:
    from openlineage.common.sql import DbTableMeta


class DbColumn(RedactMixin):
    _skip_redact: ClassVar[list[str]] = ["name", "type", "ordinal_position"]

    def __init__(
        self,
        name: str,
        type: str,
        description: str | None = None,
        ordinal_position: int | None = None,
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
    _skip_redact: ClassVar[list[str]] = ["schema_name", "table_name"]

    def __init__(
        self,
        schema_name: str,
        table_name: "DbTableMeta",
        columns: list[DbColumn],
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
