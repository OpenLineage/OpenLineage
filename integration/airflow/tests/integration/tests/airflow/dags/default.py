# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.dates import days_ago
import attr

from openlineage.airflow.extractors.base import OperatorLineage
from openlineage.client.facet import BaseFacet, ParentRunFacet, SqlJobFacet
from openlineage.client.run import Dataset


INPUTS = [Dataset(namespace="database://host:port", name="inputtable")]
OUTPUTS = [Dataset(namespace="database://host:port", name="inputtable")]
RUN_FACETS = {
    "parent": ParentRunFacet.create(
        "3bb703d1-09c1-4a42-8da5-35a0b3216072", "namespace", "parentjob"
    )
}
JOB_FACETS = {"sql": SqlJobFacet(query="SELECT * FROM inputtable")}


@attr.s
class CompleteRunFacet(BaseFacet):
    finished: bool = attr.ib()


class ExampleOperator(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=JOB_FACETS,
        )

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets={"complete": CompleteRunFacet(True)},
        )


default_args = {
    "owner": "datascience",
    "depends_on_past": False,
    "start_date": days_ago(7),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
}

dag = DAG(
    "default_extractor_dag",
    schedule_interval="@once",
    default_args=default_args,
    description="Determines the popular day of week orders are placed.",
)


t1 = ExampleOperator(task_id="default_operator_first", dag=dag)

t2 = ExampleOperator(task_id="default_operator_second", dag=dag)

t1 >> t2
