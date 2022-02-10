import os
import inspect
import logging
from typing import Optional, List, Callable

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.facet import SourceCodeJobFacet


log = logging.getLogger(__name__)


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
        if os.environ.get(
            "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "False"
        ).lower() in ('true', '1', 't'):
            return None

        source_code = self.get_source_code(self.operator.python_callable)
        if source_code:
            return TaskMetadata(
                name=f"{self.operator.dag_id}.{self.operator.task_id}",
                job_facets={
                    "sourceCode": SourceCodeJobFacet(
                        "python",
                        # We're on worker and should have access to DAG files
                        source_code
                    )
                }
            )

    def get_source_code(self, callable: Callable) -> Optional[str]:
        try:
            return inspect.getsource(callable)
        except TypeError:
            # Trying to extract source code of builtin_function_or_method
            return str(callable)
        except OSError:
            log.exception(f"Can't get source code facet of PythonOperator {self.operator.task_id}")
