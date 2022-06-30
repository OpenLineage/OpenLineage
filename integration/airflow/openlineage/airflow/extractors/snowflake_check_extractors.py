# SPDX-License-Identifier: Apache-2.0

import logging
from abc import abstractmethod
from typing import List, Optional, Dict

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.extractors.dbapi_utils import get_table_schemas
from openlineage.airflow.utils import get_connection
from openlineage.client.facet import SqlJobFacet, ExternalQueryRunFacet
from openlineage.common.dataset import Source
from openlineage.common.sql import SqlMeta, parse

from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor
from openlineage.common.dataset import Dataset, Field

from openlineage.airflow.utils import (
    build_column_check_facets,
    build_table_check_facets
)


logger = logging.getLogger(__file__)

class BaseSnowflakeCheckExtractor(SnowflakeExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    def extract_on_complete(self, task_instance):
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

    def extract(self):
        return

    @abstractmethod
    def _build_facets(self) -> dict:
        pass


class SnowflakeCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeCheckOperator"]

    def _build_facets(self) -> dict:
        return None


class SnowflakeValueCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeValueCheckOperator"]

    def _build_facets(self) -> dict:
        return None


class SnowflakeThresholdCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeThresholdCheckOperator"]

    def _build_facets(self) -> dict:
        return None


class SnowflakeIntervalCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeIntervalCheckOperator"]

    def _build_facets(self) -> dict:
        return None


class SnowflakeColumnCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeColumnCheckOperator"]

    def _build_facets(self) -> dict:
        column_mapping = self.operator.column_mapping
        return build_column_check_facets(column_mapping)


class SnowflakeTableCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeTableCheckOperator"]

    def _build_facets(self) -> dict:
        checks = self.operator.checks
        return build_table_check_facets(checks)
