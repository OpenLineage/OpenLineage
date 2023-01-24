# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import get_job_name
from openlineage.client.facet import SqlJobFacet
from openlineage.common.provider.redshift_data import RedshiftDataDatasetsProvider
from openlineage.common.sql import SqlMeta, parse


class RedshiftDataExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["RedshiftDataOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        return None

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        self.log.debug(f"extract_on_complete({task_instance})")
        job_facets = {"sql": SqlJobFacet(self.operator.sql)}

        self.log.debug(f"Sending SQL to parser: {self.operator.sql}")
        sql_meta: Optional[SqlMeta] = parse(
            self.operator.sql,
            dialect=self.dialect,
            default_schema=self.default_schema
        )
        self.log.debug(f"Got meta {sql_meta}")
        try:
            redshift_job_id = self._get_xcom_redshift_job_id(task_instance)
            if redshift_job_id is None:
                raise Exception(
                    "Xcom could not resolve Redshift job id. Job may have failed."
                )
        except Exception as e:
            self.log.error(f"Cannot retrieve job details from {e}", exc_info=True)
            return TaskMetadata(
                name=get_job_name(task=self.operator),
                run_facets={},
                job_facets=job_facets,
            )

        client = self.operator.hook.conn

        redshift_details = [
            "database",
            "cluster_identifier",
            "db_user",
            "secret_arn",
            "region",
        ]

        connection_details = {
            detail: getattr(self.operator, detail) for detail in redshift_details
        }

        stats = RedshiftDataDatasetsProvider(
            client=client, connection_details=connection_details
        ).get_facets(
            job_id=redshift_job_id,
            inputs=sql_meta.in_tables if sql_meta else [],
            outputs=sql_meta.out_tables if sql_meta else [],
        )

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[ds.to_openlineage_dataset() for ds in stats.inputs],
            outputs=[ds.to_openlineage_dataset() for ds in stats.output],
            run_facets=stats.run_facets,
            job_facets={"sql": SqlJobFacet(self.operator.sql)},
        )

    @property
    def dialect(self):
        return "redshift"

    @property
    def default_schema(self):
        # TODO: check default schema in redshift
        return "public"

    def _get_xcom_redshift_job_id(self, task_instance):
        redshift_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id)

        self.log.debug(f"redshift job id: {redshift_job_id}")
        return redshift_job_id
