# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List
from urllib.parse import urlparse

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.utils import try_import_from_string


class TrinoExtractor(SqlExtractor):
    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
        "table_catalog",
    ]
    _is_information_schema_cross_db = True
    _allow_trailing_semicolon = False

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["TrinoOperator"]

    @property
    def default_schema(self):
        return self.conn.schema

    def _get_scheme(self):
        return "trino"

    def _get_database(self) -> str:
        # hive is default in airflow trino provider, not in trino package
        return self.conn.extra_dejson.get("catalog", "hive")

    def _get_authority(self) -> str:
        if self.conn.host and self.conn.port:
            return f"{self.conn.host}:{self.conn.port}"
        else:
            parsed = urlparse(self.conn.get_uri())
            return f"{parsed.hostname}:{parsed.port}"

    def _get_hook(self):
        TrinoHook = try_import_from_string("airflow.providers.trino.hooks.trino.TrinoHook")
        return TrinoHook(trino_conn_id=self.operator.trino_conn_id)
