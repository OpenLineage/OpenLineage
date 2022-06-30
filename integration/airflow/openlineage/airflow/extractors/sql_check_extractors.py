# SPDX-License-Identifier: Apache-2.0

import logging
from abc import abstractmethod
from typing import List, Optional, Dict
from sqlalchemy import MetaData, Table

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.extractors.dbapi_utils import get_table_schemas
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.client.facet import SqlJobFacet
from openlineage.common.sql import SqlMeta, parse
from openlineage.common.dataset import Dataset, Source, Field
from openlineage.airflow.utils import (
    build_column_check_facets,
    build_table_check_facets,
    get_connection
)

logger = logging.getLogger(__name__)


class BaseSqlCheckExtractor(SqlExtractor):
    default_schema = 'public'

    def __init__(self, operator):
        super().__init__(operator)

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        job_facets = {"sql": SqlJobFacet(self.operator.sql)}
        run_facets: Dict = {}

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
                job_facets=job_facets,
            )

        # (2) Construct source object
        source = Source(
            scheme=self._scheme,
            authority=self._get_authority(),
            connection_url=self._get_connection_uri(),
        )

        database = getattr(self.operator, "database", None)
        if not database:
            database = self._get_database()

        # (3) Map input / output tables to dataset objects with source set
        # as the current connection. We need to also fetch the schema for the
        # input tables to format the dataset name as:
        # {schema_name}.{table_name}
        inputs, outputs = get_table_schemas(
            self.hook,
            source,
            database,
            self._get_in_query(sql_meta.in_tables) if sql_meta.in_tables else None,
            self._get_out_query(sql_meta.out_tables) if sql_meta.out_tables else None,
        )

        for ds in inputs:
            ds.input_facets = self._build_facets()

        db_specific_run_facets = self._get_db_specific_run_facets(
            source, inputs, outputs
        )

        run_facets = {**run_facets, **db_specific_run_facets}

        return TaskMetadata(
            name=task_name,
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def _get_input_tables(self, source, database, sql_meta):
        inputs = []
        for in_table_schema in self._get_table_schemas(sql_meta.in_tables):
            table_name = self._normalize_identifiers(in_table_schema.table_name.name)
            ds = Dataset.from_table(
                source=source,
                table_name=table_name,
                schema_name=in_table_schema.schema_name,
                database_name=database
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

        for ds in inputs:
            ds.input_facets = self._build_facets()

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


    @abstractmethod
    def _build_facets(self) -> dict:
        pass


class SqlCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLCheckOperator']

    def _build_facets(self) -> dict:
        return


class SqlValueCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLValueCheckOperator']

    def _build_facets(self) -> dict:
        return


class SqlThresholdCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLThresholdCheckOperator']

    def _build_facets(self) -> dict:
        return


class SqlIntervalCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLIntervalCheckOperator']

    def _build_facets(self) -> dict:
        return


class SqlColumnCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLColumnCheckOperator']

    def _build_facets(self) -> dict:
        column_mapping = self.operator.column_mapping
        return build_column_check_facets(column_mapping)


class SqlTableCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLTableCheckOperator']

    def _build_facets(self) -> dict:
        checks = self.operator.checks
        return build_table_check_facets(checks)
