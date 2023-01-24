# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List
from urllib.parse import urlparse

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.utils import try_import_from_string


class MySqlExtractor(SqlExtractor):
    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "column_type",
    ]

    @property
    def dialect(self):
        return "mysql"

    @property
    def default_schema(self):
        return None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["MySqlOperator"]

    def _get_scheme(self) -> str:
        return "mysql"

    def _get_database(self) -> None:
        # MySQL does not have database concept.
        return None

    def _get_authority(self) -> str:
        if self.conn.host and self.conn.port:
            return f"{self.conn.host}:{self.conn.port}"
        else:
            parsed = urlparse(self.conn.get_uri())
            return f"{parsed.hostname}:{parsed.port}"

    def _get_hook(self):
        MySqlHook = try_import_from_string("airflow.providers.mysql.hooks.mysql.MySqlHook")
        return MySqlHook(
            mysql_conn_id=self.operator.mysql_conn_id, schema=self.operator.database
        )

    @staticmethod
    def _normalize_name(name: str) -> str:
        return name.upper()
