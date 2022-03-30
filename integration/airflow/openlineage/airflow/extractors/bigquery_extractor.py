# SPDX-License-Identifier: Apache-2.0.

import json
import logging
import traceback
from typing import Optional, List

import attr

from openlineage.client.facet import SqlJobFacet
from openlineage.common.provider.bigquery import BigQueryDatasetsProvider, BigQueryErrorRunFacet
from openlineage.common.sql import SqlParser

from openlineage.airflow.extractors.base import (
    BaseExtractor,
    TaskMetadata
)
from openlineage.airflow.utils import get_job_name

_BIGQUERY_CONN_URL = 'bigquery'

log = logging.getLogger(__name__)


@attr.s
class SqlContext:
    """Internal SQL context for holding query parser results"""
    sql: str = attr.ib()
    inputs: Optional[str] = attr.ib(default=None)
    outputs: Optional[str] = attr.ib(default=None)
    parser_error: Optional[str] = attr.ib(default=None)


class BigQueryExtractor(BaseExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryOperator', 'BigQueryExecuteQueryOperator']

    def extract(self) -> Optional[TaskMetadata]:
        return None

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        log.debug(f"extract_on_complete({task_instance})")
        context = self.parse_sql_context()

        try:
            bigquery_job_id = self._get_xcom_bigquery_job_id(task_instance)
            if bigquery_job_id is None:
                raise Exception(
                    "Xcom could not resolve BigQuery job id. Job may have failed."
                )
        except Exception as e:
            log.error(
                f"Cannot retrieve job details from BigQuery.Client. {e}", exc_info=True
            )
            return TaskMetadata(
                name=get_job_name(task=self.operator),
                run_facets={
                    "bigQuery_error": BigQueryErrorRunFacet(
                        clientError=f"{e}: {traceback.format_exc()}",
                        parserError=context.parser_error
                    )
                }
            )

        stats = BigQueryDatasetsProvider().get_facets(bigquery_job_id)
        inputs = stats.inputs
        output = stats.output
        run_facets = stats.run_facets
        job_facets = {
            "sql": SqlJobFacet(context.sql)
        }

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[output.to_openlineage_dataset()] if output else [],
            run_facets=run_facets,
            job_facets=job_facets
        )

    def _get_xcom_bigquery_job_id(self, task_instance):
        bigquery_job_id = task_instance.xcom_pull(
            task_ids=task_instance.task_id, key='job_id')

        log.debug(f"bigquery_job_id: {bigquery_job_id}")
        return bigquery_job_id

    def parse_sql_context(self) -> SqlContext:
        try:
            sql_meta = SqlParser.parse(self.operator.sql, None)
            log.debug(f"bigquery sql parsed and obtained meta: {sql_meta}")
            return SqlContext(
                sql=self.operator.sql,
                inputs=json.dumps(
                    [in_table.name for in_table in sql_meta.in_tables]
                ),
                outputs=json.dumps(
                    [out_table.name for out_table in sql_meta.out_tables]
                )
            )
        except Exception as e:
            log.error(f"Cannot parse sql query. {e}",
                      exc_info=True)
            return SqlContext(
                sql=self.operator.sql,
                parser_error=f'{e}: {traceback.format_exc()}'
            )
