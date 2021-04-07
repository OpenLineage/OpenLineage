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

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import bigquery

from marquez_airflow.extractors import (
    BaseExtractor,
    StepMetadata,
    Source,
    Dataset
)
from marquez_airflow.models import (
    DbTableName,
    DbTableSchema,
    DbColumn
)
from marquez_airflow.extractors.sql import SqlParser
from marquez_airflow.utils import (
    get_job_name
)

# BIGQUERY DAGs doesn't use this.
# Required to pass marquez server validation.
_BIGQUERY_CONN_URL = \
    'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443'

log = logging.getLogger(__name__)


class BigQueryExtractor(BaseExtractor):
    operator_class = BigQueryOperator

    def __init__(self, operator):
        super().__init__(operator)

    def _source(self) -> Source:
        conn_id = self.operator.bigquery_conn_id
        return Source(
            type="BIGQUERY",
            name=conn_id,
            connection_url=_BIGQUERY_CONN_URL)

    def extract(self) -> StepMetadata:
        return None

    def _bq_table_name(self, bq_table):
        project = bq_table.get('projectId')
        dataset = bq_table.get('datasetId')
        table = bq_table.get('tableId')
        return f"{project}.{dataset}.{table}"

    def extract_on_complete(self, task_instance) -> StepMetadata:
        log.debug(f"extract_on_complete({task_instance})")
        context = self.parse_sql_context()
        source = self._source()

        try:
            bigquery_job_id = self._get_xcom_bigquery_job_id(task_instance)
            context['bigquery.job_id'] = bigquery_job_id
            if bigquery_job_id is None:
                raise Exception("Xcom could not resolve BigQuery job id." +
                                "Job may have failed.")
        except Exception as e:
            log.error(f"Cannot retrieve job details from BigQuery.Client. {e}",
                      exc_info=True)
            context['bigquery.extractor.client_error'] = \
                f"{e}: {traceback.format_exc()}"
            return StepMetadata(
                name=get_job_name(task=self.operator),
                context=context,
                inputs=None,
                outputs=None
            )

        inputs = None
        outputs = None
        try:
            client = bigquery.Client()
            try:
                job = client.get_job(job_id=bigquery_job_id)
                job_properties_str = json.dumps(job._properties)
                context['bigquery.job_properties'] = job_properties_str

                inputs = self._get_input_from_bq(job, context, source, client)
                outputs = self._get_output_from_bq(job, source, client)
            finally:
                # Ensure client has close() defined, otherwise ignore.
                # NOTE: close() was introduced in python-bigquery v1.23.0
                if hasattr(client, "close"):
                    client.close()
        except Exception as e:
            log.error(f"Cannot retrieve job details from BigQuery.Client. {e}",
                      exc_info=True)
            context['bigquery.extractor.error'] = \
                f"{e}: {traceback.format_exc()}"

        return StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=outputs,
            context=context
        )

    def _get_input_from_bq(self, job, context, source, client):
        if not job._properties.get('statistics')\
              or not job._properties.get('statistics').get('query')\
              or not job._properties.get('statistics').get('query')\
                  .get('referencedTables'):
            return None

        bq_input_tables = job._properties.get('statistics')\
            .get('query')\
            .get('referencedTables')

        input_table_names = [
            self._bq_table_name(bq_t) for bq_t in bq_input_tables
        ]
        try:
            return [
                Dataset.from_table_schema(
                    source=source,
                    table_schema=table_schema
                )
                for table_schema in self._get_table_schemas(
                    input_table_names, client
                )
            ]
        except Exception as e:
            log.warn(f'Could not extract schema from bigquery. {e}')
            context['bigquery.extractor.bq_schema_error'] = \
                f'{e}: {traceback.format_exc()}'
            return [
                Dataset.from_table(source, table)
                for table in input_table_names
            ]

    def _get_output_from_bq(self, job, source, client):
        if not job._properties.get('configuration') or\
              not job._properties.get('configuration').get('query') or\
              not job._properties.get('configuration').get('query')\
                  .get('destinationTable'):
            return None

        bq_output_table = job._properties.get('configuration') \
            .get('query') \
            .get('destinationTable')
        output_table_name = self._bq_table_name(bq_output_table)
        table_schema = self._get_table_safely(output_table_name, client)
        if table_schema:
            return [Dataset.from_table_schema(
                source=source,
                table_schema=table_schema
            )]
        else:
            log.warn("Could not resolve output table from bq")
            return [
                Dataset.from_table(source, output_table_name)
            ]

    def _get_table_safely(self, output_table_name, client):
        try:
            return self._get_table(output_table_name, client)
        except Exception as e:
            log.warning(f'Could not extract output schema from bigquery. {e}')
        return None

    def _get_table_schemas(self, tables: [str], client: bigquery.Client) \
            -> [DbTableSchema]:
        # Avoid querying postgres by returning an empty array
        # if no tables have been provided.
        if not tables:
            return []

        return [self._get_table(table, client) for table in tables]

    def _get_table(self, table: str, client: bigquery.Client) -> DbTableSchema:
        bq_table = client.get_table(table)
        if not bq_table._properties:
            return
        table = bq_table._properties

        if not table.get('schema') or not table.get('schema').get('fields'):
            return

        fields = table.get('schema').get('fields')
        columns = [DbColumn(
            name=fields[i].get('name'),
            type=fields[i].get('type'),
            description=fields[i].get('description'),
            ordinal_position=i
        ) for i in range(len(fields))]
        self.log.info(DbTableName(table.get('tableReference').get('tableId')))
        return DbTableSchema(
            schema_name=table.get('tableReference').get('projectId') + '.' +
            table.get('tableReference').get('datasetId'),
            table_name=DbTableName(table.get('tableReference').get('tableId')),
            columns=columns
        )

    def _get_xcom_bigquery_job_id(self, task_instance):
        bigquery_job_id = task_instance.xcom_pull(
            task_ids=task_instance.task_id, key='job_id')

        log.info(f"bigquery_job_id: {bigquery_job_id}")

        return bigquery_job_id

    def parse_sql_context(self):
        context = {
            'sql': self.operator.sql,
        }
        try:
            sql_meta = SqlParser.parse(self.operator.sql)
            log.debug(f"bigquery sql parsed and obtained meta: {sql_meta}")
            context['bigquery.sql.parsed.inputs'] = json.dumps(
                [in_table.name for in_table in sql_meta.in_tables]
            )
            context['bigquery.sql.parsed.outputs'] = json.dumps(
                [out_table.name for out_table in sql_meta.out_tables]
            )
        except Exception as e:
            log.error(f"Cannot parse sql query. {e}",
                      exc_info=True)
            context['bigquery.extractor.sql_parser_error'] = \
                f'{e}: {traceback.format_exc()}'
        self.log.info(context)
        return context
