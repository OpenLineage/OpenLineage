# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import inspect
import os
from typing import Callable, Dict, List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import get_unknown_source_attribute_run_facet
from openlineage.client.facet_v2 import source_code_job


class PythonExtractor(BaseExtractor):
    """
    This extractor provides visibility on what particular task does by extracting
    executed source code and putting it into SourceCodeJobFacet. It does not extract
    datasets.
    """

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["PythonOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        collect_source = os.environ.get("OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "True").lower() not in (
            "true",
            "1",
            "t",
        )

        source_code = self.get_source_code(self.operator.python_callable)
        job_facet: Dict = {}
        if collect_source and source_code:
            job_facet = {
                "sourceCode": source_code_job.SourceCodeJobFacet(
                    "python",
                    # We're on worker and should have access to DAG files
                    source_code,
                )
            }
        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            job_facets=job_facet,
            # The PythonOperator is recorded as an "unknownSource" even though we have an extractor,
            # as the <i>data lineage</i> cannot be determined from the operator directly.
            run_facets=get_unknown_source_attribute_run_facet(task=self.operator, name="PythonOperator"),
        )

    def get_source_code(self, callable: Callable) -> Optional[str]:
        try:
            return inspect.getsource(callable)
        except TypeError:
            # Trying to extract source code of builtin_function_or_method
            return str(callable)
        except OSError:
            self.log.exception(f"Can't get source code facet of PythonOperator {self.operator.task_id}")
        return None
