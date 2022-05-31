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

<<<<<<< HEAD
=======
    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        job_facets = {
            'sql': SqlJobFacet(self.operator.sql)
        }

        return self.build_metadata()

    def build_metadata(self) -> TaskMetadata:
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

    def _get_input_tables(self, source, database, sql_meta):
        return [
            Dataset.from_table(
                source=source,
                table_name=in_table_schema.table_name.name,
                schema_name=in_table_schema.schema_name,
                database_name=database
            ) for in_table_schema in self._get_table_schemas(
                sql_meta.in_tables
            )
        ]

>>>>>>> 2131dddb (Refactor PostgresExtractor to build metadata outside of extract or extract_on_complete, so check extractors inheriting can run that code only on complete.)
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
