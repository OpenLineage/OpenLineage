import re
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Tuple

import agate
import google.cloud.exceptions
import google.auth.exceptions
from dataclasses import dataclass

from dbt.adapters.bigquery.connections import BigQueryAdapterResponse, BigQueryCredentials, \
    BigQueryConnectionManager
from dbt.clients import agate_helper
from dbt.exceptions import DatabaseException, RuntimeException
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import format_bytes, format_rows_number

from marquez.provider.bigquery import BigQueryDatasetsProvider
from marquez.sql import SqlParser
from openlineage.client import OpenLineageClientOptions, OpenLineageClient
from openlineage.facet import SourceCodeLocationJobFacet, SqlJobFacet
from openlineage.run import RunEvent, RunState, Run, Job, Dataset
import requests


@dataclass
class OpenLineageBigQueryCredentials(BigQueryCredentials):
    """setting openlineage_url in dbt profile is required"""
    openlineage_url: Optional[str] = None
    openlineage_timeout: float = 5.0
    openlineage_api_key: Optional[str] = None

    @property
    def type(self) -> str:
        return 'openlineage_bigquery'


@dataclass
class RunMeta:
    """Runtime information about model set up on start event that
    needs to be shared with complete/fail events
    """
    run_id: str
    namespace: str
    job_name: str


BQ_QUERY_JOB_SPLIT = '-----Query Job SQL Follows-----'
PRODUCER = "marquez-dbt-bigquery/0.16.1rc1"


class OpenLineageBigQueryConnectionManager(BigQueryConnectionManager):
    """Emitting openlineage events here allows us to handle
    errors and send fail events for them
    """
    TYPE = 'openlineage_bigquery'

    @classmethod
    def handle_error(cls, error, message):
        error_msg = "\n".join([item['message'] for item in error.errors])
        raise DatabaseException(error_msg)

    def get_openlineage_client(self):
        if not hasattr(self, '_openlineage_client'):
            creds = self.profile.credentials
            self._openlineage_client = OpenLineageClient(
                creds.openlineage_url,
                OpenLineageClientOptions(
                    timeout=creds.openlineage_timeout,
                    api_key=creds.openlineage_api_key
                )
            )
        return self._openlineage_client

    def emit(self, event: RunEvent):
        try:
            self.get_openlineage_client().emit(event)
        except requests.exceptions.RequestException:
            logger.debug(
                f'openlineage {event.eventType.name} event with run_id: {event.run.runId}'
            )

    def emit_start(self, model, run_started_at: str):
        run_id = str(uuid.uuid4())

        inputs = []
        try:
            sql_meta = SqlParser.parse(model['compiled_sql'], None)
            inputs = [
                in_table.qualified_name for in_table in sql_meta.in_tables
            ]
        except Exception as e:
            logger.error(f"Cannot parse biquery sql. {e}", exc_info=True)

        # set up metadata information to use in complete/fail events
        if not hasattr(self, '_meta'):
            self._meta = dict()
        meta = RunMeta(
            run_id,
            model['fqn'][0],
            model['unique_id']
        )
        self._meta[model['unique_id']] = meta
        self._meta[run_id] = meta
        self._bigquery_job_id = None

        output_relation_name = model['relation_name'].replace('`', "")

        self.emit(RunEvent(
            eventType=RunState.START,
            eventTime=run_started_at,
            run=Run(runId=run_id),
            job=Job(
                namespace=meta.namespace,
                name=meta.job_name,
                facets={
                    "sourceCodeLocation": SourceCodeLocationJobFacet(
                        "",
                        model['original_file_path']
                    ),
                    "sql": SqlJobFacet(model['compiled_sql'])
                }
            ),
            producer=PRODUCER,
            inputs=sorted([
                Dataset(
                    namespace='bigquery',
                    name=relation,
                    facets={}
                ) for relation in inputs
            ], key=lambda x: x.name),
            outputs=[
                Dataset(namespace='bigquery', name=output_relation_name)
            ]
        ))
        return run_id

    def emit_complete(self, run_id):
        meta = self._meta[run_id]

        # time.sleep(1)
        if hasattr(self, '_bigquery_job_id') and self._bigquery_job_id is not None:
            bq_job_id = self._bigquery_job_id
        else:
            logger.error(f'failed to find bigquery job id in run {run_id}')
            event = RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=datetime.now().isoformat(),
                run=Run(runId=run_id),
                job=Job(namespace=meta.namespace, name=meta.job_name),
                producer=PRODUCER,
                inputs=[],
                outputs=[]
            )
            self.get_openlineage_client().emit(event)
            return

        stats = BigQueryDatasetsProvider(
            self.get_thread_connection().handle, logger
        ).get_facets(bq_job_id)

        inputs = stats.inputs
        output = stats.output
        run_facets = stats.run_facets

        self.emit(RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat(),
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=meta.namespace, name=meta.job_name),
            producer=PRODUCER,
            inputs=[input.to_openlineage_dataset() for input in inputs],
            outputs=[output.to_openlineage_dataset()]
        ))

    def emit_failed(self, sql):
        res = re.search(r'"node_id": "(.*)"', sql)
        if not res:
            logger.error("can't emit OpenLineage event when run failed: can't find run id")
            return
        if not hasattr(self, '_meta') or res.group(1) not in self._meta:
            logger.error("can't emit OpenLineage event when run failed: "
                         "no meta info or no run id in meta info")
        meta = self._meta[res.group(1)]
        self.emit(RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now().isoformat(),
            run=Run(runId=meta.run_id),
            job=Job(namespace=meta.namespace, name=meta.job_name),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))

    def execute(
        self, sql, auto_begin=False, fetch=None
    ) -> Tuple[BigQueryAdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        query_job, iterator = self.raw_execute(sql, fetch=fetch)
        self._bigquery_job_id = query_job.job_id

        if fetch:
            table = self.get_table_from_response(iterator)
        else:
            table = agate_helper.empty_table()

        message = 'OK'
        code = None
        num_rows = None
        bytes_processed = None

        if query_job.statement_type == 'CREATE_VIEW':
            code = 'CREATE VIEW'

        elif query_job.statement_type == 'CREATE_TABLE_AS_SELECT':
            conn = self.get_thread_connection()
            client = conn.handle
            query_table = client.get_table(query_job.destination)
            code = 'CREATE TABLE'
            num_rows = query_table.num_rows
            bytes_processed = query_job.total_bytes_processed
            message = '{} ({} rows, {} processed)'.format(
                code,
                format_rows_number(num_rows),
                format_bytes(bytes_processed)
            )

        elif query_job.statement_type == 'SCRIPT':
            code = 'SCRIPT'
            bytes_processed = query_job.total_bytes_processed
            message = f'{code} ({format_bytes(bytes_processed)} processed)'

        elif query_job.statement_type in ['INSERT', 'DELETE', 'MERGE']:
            code = query_job.statement_type
            num_rows = query_job.num_dml_affected_rows
            bytes_processed = query_job.total_bytes_processed
            message = '{} ({} rows, {} processed)'.format(
                code,
                format_rows_number(num_rows),
                format_bytes(bytes_processed),
            )

        response = BigQueryAdapterResponse(
            _message=message,
            rows_affected=num_rows,
            code=code,
            bytes_processed=bytes_processed
        )

        return response, table

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            self.emit_failed(sql)
            message = "Bad request while running query"
            self.handle_error(e, message)

        except google.cloud.exceptions.Forbidden as e:
            self.emit_failed(sql)
            message = "Access denied while running query"
            self.handle_error(e, message)

        except google.auth.exceptions.RefreshError as e:
            self.emit_failed(sql)
            message = "Unable to generate access token, if you're using " \
                      "impersonate_service_account, make sure your " \
                      'initial account has the "roles/' \
                      'iam.serviceAccountTokenCreator" role on the ' \
                      'account you are trying to impersonate.\n\n' \
                      f'{str(e)}'
            raise RuntimeException(message)

        except Exception as e:
            self.emit_failed(sql)
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            exc_message = str(e)
            # the google bigquery library likes to add the query log, which we
            # don't want to log. Hopefully they never change this!
            if BQ_QUERY_JOB_SPLIT in exc_message:
                exc_message = exc_message.split(BQ_QUERY_JOB_SPLIT)[0].strip()
            raise RuntimeException(exc_message)
