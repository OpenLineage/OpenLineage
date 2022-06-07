import inspect
import logging
import os

from typing import Callable, List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.run import Dataset
from openlineage.client.facet import SourceCodeJobFacet
from openlineage.integration.common.openlineage.common.provider.livemaps import (
    LiveMapsPythonDecoratedFacet,
)
from openlineage.client.python.openlineage.common.constants import (
    DEFAULT_NAMESPACE_NAME,
)


log = logging.getLogger(__name__)


_DAG_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", None)
if not _DAG_NAMESPACE:
    _DAG_NAMESPACE = os.getenv("MARQUEZ_NAMESPACE", DEFAULT_NAMESPACE_NAME)


class LiveMapsPythonDecoratedExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["_PythonDecoratedOperator"]

    def extract(self) -> Optional[TaskMetadata]:

        collect_source = True
        if os.environ.get(
            "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "False"
        ).lower() in ("true", "1", "t"):
            collect_source = False

        source_code = self.get_source_code(self.operator.python_callable)
        job_facet = {}
        if collect_source and source_code:
            job_facet = {
                "sourceCode": SourceCodeJobFacet(
                    "python",
                    # We're on worker and should have access to DAG files
                    source_code,
                )
            }

        _inputs = []
        if self.operator.get_inlet_defs():
            _inputs = list(
                map(
                    self.extract_inlets_and_outlets,
                    self.operator.get_inlet_defs(),
                )
            )

        _outputs = []
        if self.operator.get_outlet_defs():
            _outputs = list(
                map(
                    self.extract_inlets_and_outlets,
                    self.operator.get_outlet_defs(),
                )
            )

        run_facet = {
            "manualLineage": LiveMapsPythonDecoratedFacet(
                database=self.operator.get_inlet_defs()[0]["database"],
                cluster=self.operator.get_inlet_defs()[0]["cluster"],
                connectionUrl=self.operator.get_inlet_defs()[0][
                    "connectionUrl"
                ],
                target=self.operator.get_inlet_defs()[0]["target"],
                source=self.operator.get_inlet_defs()[0]["source"],
            )
            if self.operator.get_inlet_defs()
            else LiveMapsPythonDecoratedFacet(
                database=self.operator.get_outlet_defs()[0]["database"],
                cluster=self.operator.get_outlet_defs()[0]["cluster"],
                connectionUrl=self.operator.get_outlet_defs()[0][
                    "connectionUrl"
                ],
                target=self.operator.get_outlet_defs()[0]["target"],
                source=self.operator.get_outlet_defs()[0]["source"],
            )
        }

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=_inputs,
            outputs=_outputs,
            job_facets=job_facet,
            run_facets=run_facet,
        )

    def get_source_code(self, callable: Callable) -> Optional[str]:
        try:
            return inspect.getsource(callable)
        except TypeError:
            # Trying to extract source code of builtin_function_or_method
            return str(callable)
        except OSError:
            log.exception(
                f"Can't get source code facet of PythonOperator {self.operator.task_id}"
            )

    def extract_inlets_and_outlets(self, properties):
        return Dataset(
            namespace=_DAG_NAMESPACE,
            name=properties["name"],
        )
