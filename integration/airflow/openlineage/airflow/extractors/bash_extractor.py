import os
from typing import Optional, List, Dict

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.facets import UnknownOperatorAttributeRunFacet, UnknownOperatorInstance
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
        collect_source = True
        if os.environ.get(
            "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "True"
        ).lower() in ('true', '1', 't'):
            collect_source = False

        job_facet: Dict = {}
        if collect_source:
            job_facet={
                "sourceCode": SourceCodeJobFacet(
                    "bash",
                    # We're on worker and should have access to DAG files
                    self.operator.bash_command
                )
            }

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            job_facets=job_facet,
            run_facets={

                # The BashOperator is recorded as an "unknownSource" even though we have an
                # extractor, as the <i>data lineage</i> cannot be determined from the operator
                # directly.
                "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                    unknownItems=[
                        UnknownOperatorInstance(
                            name="BashOperator",
                            properties={attr: value
                                        for attr, value in self.operator.__dict__.items()}
                        )
                    ]
                )
            }
        )
