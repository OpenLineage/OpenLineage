import logging
from contextlib import closing
from typing import Dict, Iterator, List, Optional

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor
from openlineage.client.facet import SqlJobFacet
from openlineage.client.run import Dataset as InputDataset
from openlineage.common.dataset import Dataset, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta

log = logging.getLogger(__name__)


class S3ToSnowflakeExtractor(SnowflakeExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['S3ToSnowflakeOperator']

    def _get_hook(self):
        if hasattr(self.operator, "get_db_hook"):
            return self.operator.get_db_hook()
        else:
            return self.operator.get_hook()

    def execute_query_on_hook(self, hook, query) -> Iterator[tuple]:
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                return cursor.execute(query).fetchall()

    def _get_table(self, table: str) -> Optional[DbTableSchema]:
        self.log.debug("Getting table details")
        sql = self._information_schema_query([table])
        self.log.debug("Executing query for schema: {}".format(sql))
        fields = self.execute_query_on_hook(hook=self._get_hook(), query=sql)
        self.log.debug(type(fields))
        self.log.debug("Table Structure: {}".format(fields))

        if not fields:
            return None

        new_fields = list(fields)
        schema_name = new_fields[0][0]
        self.log.debug("Database Schema is {}".format(schema_name))

        columns = [
            DbColumn(
                name=new_fields[i][2],
                type=new_fields[i][4],
                ordinal_position=i,
            )
            for i in range(len(new_fields))
        ]

        return DbTableSchema(
            schema_name=schema_name,
            table_name=table,
            columns=columns,
        )

    def _get_table_schemas(self, tables: List[DbTableMeta]) -> List[DbTableSchema]:
        if not tables:
            return []
        return [self._get_table(table) for table in tables]

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        inputs = InputDataset(
            # since no parameter for s3 bucket, relying on stage for namespace
            namespace="snowflake_stage_{}".format(self.operator.stage),
            name=self.operator.s3_keys[0],
            facets={}
        )

        source = Source(
            scheme=self._get_scheme(),
            authority=self._get_authority(),
            connection_url=self.get_connection_uri(self.conn)
        )

        database = self.operator.database
        if not database:
            database = self._get_database()

        out_table = DbTableMeta('.'.join([self.operator.schema,
                                          self.operator.table]).upper())
        outputs = [
            Dataset.from_table_schema(
                source=source,
                table_schema=out_table_schema,
                database_name=database
            ) for out_table_schema in self._get_table_schemas(
                [out_table]
            )
        ]

        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        run_facets: Dict = {}
        job_facets: Dict = {
            "sql": SqlJobFacet(query=f"""
                        copy into {self.operator.table}
                        from {self.operator.stage}
                        files=('{self.operator.s3_keys}')
                        fileformat=({self.operator.file_format})
                    """)
        }

        db_specific_run_facets = self._get_db_specific_run_facets(
            source, inputs, outputs
        )

        run_facets = {**db_specific_run_facets}

        return TaskMetadata(
            name=task_name,
            inputs=[inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets=run_facets,
            job_facets=job_facets
        )

    def extract(self) -> Optional[TaskMetadata]:
        pass
