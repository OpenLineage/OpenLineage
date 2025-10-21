# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import traceback
from typing import List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import get_job_name, try_import_from_string
from openlineage.client.facet_v2 import sql_job
from openlineage.common.provider.bigquery import (
    BigQueryDatasetsProvider,
    BigQueryErrorRunFacet,
)

_BIGQUERY_CONN_URL = "bigquery"


class BigQueryExtractor(BaseExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["BigQueryOperator", "BigQueryExecuteQueryOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        return None

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        self.log.debug("extract_on_complete(%s)", task_instance)

        try:
            bigquery_job_id = self._get_xcom_bigquery_job_id(task_instance)
            if bigquery_job_id is None:
                raise Exception("Xcom could not resolve BigQuery job id. Job may have failed.")
        except Exception as e:
            self.log.error("Cannot retrieve job details from BigQuery.Client: %s", e, exc_info=True)
            return TaskMetadata(
                name=get_job_name(task=self.operator),
                run_facets={
                    "bigQuery_error": BigQueryErrorRunFacet(
                        clientError=f"{e}: {traceback.format_exc()}",
                    )
                },
            )

        client = self._get_client()

        stats = BigQueryDatasetsProvider(client=client).get_facets(bigquery_job_id)
        inputs = stats.inputs
        outputs = stats.outputs

        for ds in inputs:
            ds.input_facets = self._get_input_facets()

        run_facets = stats.run_facets
        job_facets = {"sql": sql_job.SQLJobFacet(self.operator.sql)}

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[o.to_openlineage_dataset() for o in outputs],
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def _get_client(self):
        # lazy-load the bigquery Client due to its slow import
        from google.cloud.bigquery import Client

        # Get client using Airflow hook - this way we use the same credentials as Airflow
        if hasattr(self.operator, "hook") and self.operator.hook:
            hook = self.operator.hook
            return hook.get_client(project_id=hook.project_id, location=hook.location)
        BigQueryHook = try_import_from_string(
            "airflow.providers.google.cloud.operators.bigquery.BigQueryHook"
        )
        if BigQueryHook is not None:
            params = {
                "gcp_conn_id": self.operator.gcp_conn_id,
                "use_legacy_sql": self.operator.use_legacy_sql,
                "location": self.operator.location,
                "impersonation_chain": self.operator.impersonation_chain,
            }
            if hasattr(self.operator, "delegate_to"):
                params["delegate_to"] = self.operator.delegate_to

            hook = BigQueryHook(**params)
            return hook.get_client(project_id=hook.project_id, location=hook.location)
        return Client()

    def _get_xcom_bigquery_job_id(self, task_instance):
        bigquery_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key="job_id")

        self.log.debug("bigquery_job_id: %s", bigquery_job_id)
        return bigquery_job_id

    def _get_input_facets(self):
        return {}
