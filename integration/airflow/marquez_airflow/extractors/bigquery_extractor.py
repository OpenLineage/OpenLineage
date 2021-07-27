# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import traceback
from typing import Optional

import attr

from airflow.models import BaseOperator

from marquez.provider.bigquery import BigQueryDatasetsProvider, BigQueryErrorRunFacet
from marquez.sql import SqlParser

from marquez_airflow.extractors.base import (
    BaseExtractor,
    StepMetadata
)
from marquez_airflow.utils import get_job_name

_BIGQUERY_CONN_URL = 'bigquery'

log = logging.getLogger(__name__)


@attr.s
class SqlContext:
    """Internal SQL context for holding query parser results"""
    sql: str = attr.ib()
    inputs: Optional[str] = attr.ib(default=None)
    outputs: Optional[str] = attr.ib(default=None)
    parser_error: Optional[str] = attr.ib(default=None)


def try_load_operator():
    try:
        from airflow.contrib.operators.bigquery_operator import BigQueryOperator
        return BigQueryOperator
    except Exception:
        log.warn('Did not find bigquery_operator library or failed to import it')
        return None


Operator = try_load_operator()


class BigQueryExtractor(BaseExtractor):
    operator_class = Operator

    def __init__(self, operator: BaseOperator):
        super().__init__(operator)

    def extract(self) -> Optional[StepMetadata]:
        return None

    def extract_on_complete(self, task_instance) -> Optional[StepMetadata]:
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
            return StepMetadata(
                name=get_job_name(task=self.operator),
                inputs=None,
                outputs=None,
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

        return StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=[output] if output else [],
            run_facets=run_facets
        )

    def _get_xcom_bigquery_job_id(self, task_instance):
        bigquery_job_id = task_instance.xcom_pull(
            task_ids=task_instance.task_id, key='job_id')

        log.info(f"bigquery_job_id: {bigquery_job_id}")
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
