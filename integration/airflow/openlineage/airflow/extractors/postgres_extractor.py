# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List
from urllib.parse import urlparse

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.utils import try_import_from_string


class PostgresExtractor(SqlExtractor):
    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "udt_name",
    ]
    _is_information_schema_cross_db = False

    # cluster-identifier
    @property
    def dialect(self):
        return "postgres"

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['PostgresOperator']

    @classmethod
    def get_connection_uri(cls, conn):
        uri = super().get_connection_uri(conn)
        if uri.startswith('postgresql'):
            uri = uri.replace('postgresql', 'postgres', 1)
        return uri

    def _get_scheme(self):
        return 'postgres'

    def _get_database(self) -> str:
        if self.conn.schema:
            return self.conn.schema
        else:
            parsed = urlparse(self.conn.get_uri())
            return f'{parsed.path}'

    def _get_authority(self) -> str:
        if self.conn.host and self.conn.port:
            return f'{self.conn.host}:{self.conn.port}'
        else:
            parsed = urlparse(self.conn.get_uri())
            return f'{parsed.hostname}:{parsed.port}'

    def _get_hook(self):
        PostgresHook = try_import_from_string(
            "airflow.providers.postgres.hooks.postgres.PostgresHook"
        )
        return PostgresHook(
            postgres_conn_id=self.operator.postgres_conn_id,
            schema=self.operator.database
        )
