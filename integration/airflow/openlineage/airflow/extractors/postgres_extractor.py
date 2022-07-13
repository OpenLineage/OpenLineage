# SPDX-License-Identifier: Apache-2.0.
import logging
from typing import List
from urllib.parse import urlparse

from openlineage.airflow.utils import (
    get_normalized_postgres_connection_uri,
    safe_import_airflow
)

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.common.sql import DbTableMeta


logger = logging.getLogger(__name__)


class PostgresExtractor(SqlExtractor):
    default_schema = 'public'

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['PostgresOperator']

    def _get_connection_uri(self):
        return get_normalized_postgres_connection_uri(self.conn)

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
        PostgresHook = safe_import_airflow(
            airflow_1_path="airflow.hooks.postgres_hook.PostgresHook",
            airflow_2_path="airflow.providers.postgres.hooks.postgres.PostgresHook"
        )
        return PostgresHook(
            postgres_conn_id=self.operator.postgres_conn_id,
            schema=self.operator.database
        )

    def _get_in_query(self, in_tables):
        return self._information_schema_query(in_tables)

    def _get_out_query(self, out_tables):
        return self._information_schema_query(out_tables)

    @staticmethod
    def _information_schema_query(tables: List[DbTableMeta]) -> str:
        table_names = ",".join(map(
            lambda name: f"'{name.name}'", tables
        ))
        return f"""
        SELECT table_schema,
        table_name,
        column_name,
        ordinal_position,
        udt_name
        FROM information_schema.columns
        WHERE table_name IN ({table_names});
        """
