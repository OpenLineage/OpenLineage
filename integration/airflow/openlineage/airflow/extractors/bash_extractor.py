import os
from typing import Optional, List

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.facet import SourceCodeJobFacet


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
        if os.environ.get(
            "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "False"
        ).lower() in ('true', '1', 't'):
            return None

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            job_facets={
                "sourceCode": SourceCodeJobFacet(
                    "bash",
                    # We're on worker and should have access to DAG files
                    self.operator.bash_command
                )
            }
        )
