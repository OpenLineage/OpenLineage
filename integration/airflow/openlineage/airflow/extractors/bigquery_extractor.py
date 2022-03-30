# SPDX-License-Identifier: Apache-2.0.

import json
import logging
import traceback
from typing import Optional, List
from airflow.version import version as AIRFLOW_VERSION

import attr
from pkg_resources import parse_version

from openlineage.client.facet import SqlJobFacet
from openlineage.common.provider.bigquery import BigQueryDatasetsProvider, BigQueryErrorRunFacet
from openlineage.common.sql import parse

from openlineage.airflow.extractors.base import (
    BaseExtractor,
    TaskMetadata
)
from openlineage.airflow.utils import get_job_name, try_import_from_string
from google.cloud.bigquery import Client

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
                raise Exception("Xcom could not resolve BigQuery job id." +
                                "Job may have failed.")
        except Exception as e:
            log.error(f"Cannot retrieve job details from BigQuery.Client. {e}",
                      exc_info=True)
            return TaskMetadata(
                name=get_job_name(task=self.operator),
                run_facets={
                    "bigQuery_error": BigQueryErrorRunFacet(
                        clientError=f"{e}: {traceback.format_exc()}",
                        parserError=context.parser_error
                    )
                }
            )

        client = self._get_client()

        stats = BigQueryDatasetsProvider(client=client).get_facets(bigquery_job_id)
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

    def _get_client(self):
        # Get client using Airflow hook - this way we use the same credentials as Airflow
        if hasattr(self.operator, 'hook') and self.operator.hook:
            hook = self.operator.hook
            return hook.get_client(
                project_id=hook.project_id,
                location=hook.location
            )
        elif parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
            BigQueryHook = try_import_from_string(
                'airflow.providers.google.cloud.operators.bigquery.BigQueryHook'
            )
            if BigQueryHook is not None:
                hook = BigQueryHook(
                    gcp_conn_id=self.operator.gcp_conn_id,
                    use_legacy_sql=self.operator.use_legacy_sql,
                    delegate_to=self.operator.delegate_to,
                    location=self.operator.location,
                    impersonation_chain=self.operator.impersonation_chain,
                )
                return hook.get_client(
                    project_id=hook.project_id,
                    location=hook.location
                )
        return Client()

    def _get_xcom_bigquery_job_id(self, task_instance):
        bigquery_job_id = task_instance.xcom_pull(
            task_ids=task_instance.task_id, key='job_id')

        log.debug(f"bigquery_job_id: {bigquery_job_id}")
        return bigquery_job_id

    def parse_sql_context(self) -> SqlContext:
        try:
            sql_meta = parse(self.operator.sql, dialect='bigquery')
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
