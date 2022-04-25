import logging
from collections import defaultdict
from contextlib import closing
from typing import Optional, List
from urllib.parse import urlparse

from openlineage.airflow.utils import (
    get_normalized_postgres_connection_uri,
    get_connection, safe_import_airflow
)
from openlineage.airflow.extractors.base import (
    BaseExtractor,
    TaskMetadata
)
from openlineage.client.facet import (
    SqlJobFacet, ExternalQueryRunFacet, ParentRunFacet, DocumentationJobFacet,
    SourceCodeLocationJobFacet, DataQualityMetricsInputDatasetFacet,
    ColumnMetric
)
from openlineage.common.models import (
    DbTableSchema,
    DbColumn
)
from openlineage.common.sql import SqlMeta, parse, DbTableMeta
from openlineage.common.dataset import Source, Dataset

logger = logging.getLogger(__name__)


class SqlCheckExtractor(BaseExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return [
            'SQLCheckOperator',
            'SQLValueCheckOperator',
            'SQLIntervalCheckOperator',
            'SQLThresholdCheckOperator',
            'BigQueryCheckOperator',
            'BigQueryValueCheckOperator',
            'BigQueryIntervalCheckOperator',
            'SnowflakeCheckOperator',
            'SnowflakeValueCheckOperator',
            'SnowflakeIntervalCheckOperator',
        ]

    def extract(self) -> TaskMetadata:
        # (1) Parse sql statement to obtain input / output tables.
        sql_meta: SqlMeta = parse(self.operator.sql, self.default_schema)

        # (2) Get database connection
        self.conn = get_connection(self._conn_id())

        # (3) Default all inputs / outputs to current connection.
        # NOTE: We'll want to look into adding support for the `database`
        # property that is used to override the one defined in the connection.
        source = Source(
            scheme=self._get_scheme(),
            authority=self._get_authority(),
            connection_url=self._get_connection_uri()
        )

        database = self.operator.database
        if not database:
            database = self._get_database()

        # (4) Map input tables to dataset objects with source set
        # as the current connection. We need to also fetch the schema for the
        # input tables to format the dataset name as:
        # {schema_name}.{table_name}
        inputs = [
            Dataset.from_table(
                source=source,
                table_name=in_table_schema.table_name.name,
                schema_name=in_table_schema.schema_name,
                database_name=database,
                input_facets=self.parse_checks(),
            ) for in_table_schema in self._get_table_schemas(
                sql_meta.in_tables
            )
        ]

        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        run_facets = {}
        job_facets = {
            'sql': SqlJobFacet(self.operator.sql)
        }

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
            outputs=[],
            run_facets=run_facets,
            job_facets=job_facets
        )

    def parse_checks(self) -> dict:
        facet_data = {
            "columnMetrics": defaultdict(dict),
            "rowCount": None
        }
        # for line in self.operator.sql:
        #    self.log.info(line)
        # REGEX parser to determine column name?
        facet_data["row_count"] = 1000
        return {
            'dataQuality': DataQualityMetricsInputDatasetFacet(**facet_data)
        }
