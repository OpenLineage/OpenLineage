# SPDX-License-Identifier: Apache-2.0.
import logging
from typing import List
from urllib.parse import urlparse

from openlineage.airflow.utils import (
    get_normalized_postgres_connection_uri,
    safe_import_airflow,
)

from openlineage.airflow.extractors.sql_extractor import SqlExtractor


logger = logging.getLogger(__name__)


class PostgresExtractor(SqlExtractor):
    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "udt_name",
    ]
    _is_information_schema_cross_db = False

    @property
    def dialect(self):
        return "postgres"

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
