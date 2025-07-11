# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from typing import Dict, List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import get_unknown_source_attribute_run_facet
from openlineage.client.facet_v2 import source_code_job


class BashExtractor(BaseExtractor):
    """
    This extractor provides visibility on what bash task does by extracting
    executed bash command and putting it into SourceCodeJobFacet. It does not extract
    datasets.
    """

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["BashOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        collect_source = os.environ.get("OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "True").lower() not in (
            "true",
            "1",
            "t",
        )

        job_facet: Dict = {}
        if collect_source:
            job_facet = {
                "sourceCode": source_code_job.SourceCodeJobFacet(
                    "bash",
                    # We're on worker and should have access to DAG files
                    self.operator.bash_command,
                )
            }

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            job_facets=job_facet,
            # The BashOperator is recorded as an "unknownSource" even though we have an extractor,
            # as the <i>data lineage</i> cannot be determined from the operator directly.
            run_facets=get_unknown_source_attribute_run_facet(task=self.operator, name="BashOperator"),
        )
