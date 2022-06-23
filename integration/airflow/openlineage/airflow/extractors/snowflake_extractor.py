# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, TYPE_CHECKING, Optional, Dict

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.extractors.dbapi_utils import get_table_schemas
from openlineage.airflow.utils import get_connection_uri, get_connection  # noqa
from openlineage.client.facet import SqlJobFacet, ExternalQueryRunFacet
from openlineage.common.dataset import Source
from openlineage.common.sql import SqlMeta, parse, DbTableMeta


if TYPE_CHECKING:
    from airflow.models import Connection


logger = logging.getLogger(__file__)


class SnowflakeExtractor(BaseExtractor):
    source_type = 'SNOWFLAKE'
    default_schema = 'PUBLIC'

    def __init__(self, operator):
        super().__init__(operator)
        self.conn: "Connection" = None
        self.hook = None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeOperator', 'SnowflakeOperatorAsync']

    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        run_facets: Dict = {}
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
                run_facets=run_facets,
                job_facets=job_facets
            )

        # (2) Get Airflow connection
        self.conn = get_connection(self._conn_id())

        # (3) Default all inputs / outputs to current connection.
        # NOTE: We'll want to look into adding support for the `database`
        # property that is used to override the one defined in the connection.
        source = Source(
            scheme='snowflake',
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

        query_ids = self._get_query_ids()
        if len(query_ids) == 1:
            run_facets['externalQuery'] = ExternalQueryRunFacet(
                externalQueryId=query_ids[0],
                source=source.name
            )
        elif len(query_ids) > 1:
            logger.warning(
                f"Found more than one query id for task {task_name}: {query_ids} "
                "This might indicate that this task might be better as multiple jobs"
            )

        return TaskMetadata(
            name=task_name,
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets=run_facets,
            job_facets=job_facets
        )

    def _information_schema_query(self, tables: List[DbTableMeta]) -> str:
        table_names = ",".join(map(
            lambda name: f"'{self._normalize_identifiers(name.name)}'", tables
        ))
        database = self.operator.database
        if not database:
            database = self._get_database()
        sql = f"""
        SELECT table_schema,
               table_name,
               column_name,
               ordinal_position,
               data_type
          FROM {database}.information_schema.columns
         WHERE table_name IN ({table_names});
        """
        return sql

    def _get_database(self) -> str:
        if hasattr(self.operator, 'database') and self.operator.database is not None:
            return self.operator.database
        return self.conn.extra_dejson.get('extra__snowflake__database', '') \
            or self.conn.extra_dejson.get('database', '')

    def _get_authority(self) -> str:
        if hasattr(self.operator, 'account') and self.operator.account is not None:
            return self.operator.account
        return self.conn.extra_dejson.get('extra__snowflake__account', '') \
            or self.conn.extra_dejson.get('account', '')

    def _get_hook(self):
        if hasattr(self.operator, 'get_db_hook'):
            return self.operator.get_db_hook()
        else:
            return self.operator.get_hook()

    def _conn_id(self):
        return self.operator.snowflake_conn_id

    def _normalize_identifiers(self, table: str):
        """
        Snowflake keeps it's table names in uppercase, so we need to normalize
        them before use: see
        https://community.snowflake.com/s/question/0D50Z00009SDHEoSAP/is-there-case-insensitivity-for-table-name-or-column-names  # noqa
        """
        return table.upper()

    def _get_connection_uri(self):
        return get_connection_uri(self.conn)

    def _get_query_ids(self) -> List[str]:
        if hasattr(self.operator, 'query_ids'):
            return self.operator.query_ids
        return []
