# SPDX-License-Identifier: Apache-2.0.
import logging
from typing import List, Optional
from urllib.parse import urlparse

from openlineage.airflow.extractors.dbapi_utils import get_table_schemas
from openlineage.airflow.utils import (
    get_normalized_postgres_connection_uri,
    get_connection, safe_import_airflow
)
from openlineage.airflow.extractors.base import (
    BaseExtractor,
    TaskMetadata
)
from openlineage.client.facet import SqlJobFacet
from openlineage.common.sql import SqlMeta, parse, DbTableMeta
from openlineage.common.dataset import Source


logger = logging.getLogger(__name__)


class PostgresExtractor(BaseExtractor):
    default_schema = 'public'

    def __init__(self, operator):
        super().__init__(operator)
        self.conn = None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['PostgresOperator']

    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        job_facets = {
            'sql': SqlJobFacet(self.operator.sql)
        }

        # (1) Parse sql statement to obtain input / output tables.
        logger.debug(f"Sending SQL to parser: {self.operator.sql}")
        sql_meta: Optional[SqlMeta] = parse(self.operator.sql, self.default_schema)
        logger.debug(f"Got meta {sql_meta}")

        if not sql_meta:
            return TaskMetadata(
                name=task_name,
                inputs=[],
                outputs=[],
                run_facets={},
                job_facets=job_facets
            )

        # (2) Get Airflow connection
        self.conn = get_connection(self._conn_id())

        # (3) Construct source object
        source = Source(
            scheme="postgres",
            authority=self._get_authority(),
            connection_url=self._get_connection_uri()
        )

        database = self.operator.database
        if not database:
            database = self._get_database()

        # (4) Map input / output tables to dataset objects with source set
        # as the current connection. We need to also fetch the schema for the
        # input tables to format the dataset name as:
        # {schema_name}.{table_name}
        inputs, outputs = get_table_schemas(
            self._get_hook(),
            source,
            database,
            self._information_schema_query(sql_meta.in_tables) if sql_meta.in_tables else None,
            self._information_schema_query(sql_meta.out_tables) if sql_meta.out_tables else None
        )

        return TaskMetadata(
            name=task_name,
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets={},
            job_facets=job_facets
        )

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

    def _conn_id(self):
        return self.operator.postgres_conn_id

    def _get_hook(self):
        PostgresHook = safe_import_airflow(
            airflow_1_path="airflow.hooks.postgres_hook.PostgresHook",
            airflow_2_path="airflow.providers.postgres.hooks.postgres.PostgresHook"
        )
        return PostgresHook(
            postgres_conn_id=self.operator.postgres_conn_id,
            schema=self.operator.database
        )

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
