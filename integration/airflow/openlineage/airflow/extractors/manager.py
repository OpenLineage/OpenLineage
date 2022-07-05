import logging
import os
from typing import Optional, Type, List

from openlineage.airflow.extractors import (
    TaskMetadata,
    BaseExtractor,
    Extractors,
)
from openlineage.airflow.facets import (
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from openlineage.airflow.utils import get_job_name
from openlineage.client.run import Dataset


class ExtractorManager:
    """Class abstracting management of custom extractors."""

    def __init__(self):
        self.extractors = {}
        self.task_to_extractor = Extractors()
        self.log = logging.getLogger()

    def add_extractor(self, operator, extractor: Type[BaseExtractor]):
        self.task_to_extractor.add_extractor(operator, extractor)

    def extract_metadata(
        self, dagrun, task, complete: bool = False, task_instance=None
    ) -> TaskMetadata:
        extractor = self._get_extractor(task)
        task_info = (
            f"task_type={task.__class__.__name__} "
            f"airflow_dag_id={task.dag_id} "
            f"task_id={task.task_id} "
            f"airflow_run_id={dagrun.run_id} "
        )
        collect_manual_lineage = False
        if os.environ.get(
            "OPENLINEAGE_EXPERIMENTAL_COLLECT_MANUAL_LINEAGE", "False"
        ).lower() in ("true", "1", "t"):
            self.log.warning("COLLECT_MANUAL_LINEAGE is experimental")
            collect_manual_lineage = True

        if extractor:
            # Extracting advanced metadata is only possible when extractor for particular operator
            # is defined. Without it, we can't extract any input or output data.
            try:
                self.log.debug(
                    f"Using extractor {extractor.__class__.__name__} {task_info}"
                )
                if (
                    len(task.get_inlet_defs())
                    or len(task.get_outlet_defs())
                    and collect_manual_lineage
                ):
                    self.log.exception(
                        f"Inputs/outputs were defined but {extractor.__class__.__name__} extractor's lineage metadata is being used. Extractor metadata is prioritized over manually defined lineage."
                    )
                if complete:
                    task_metadata = extractor.extract_on_complete(task_instance)
                else:
                    task_metadata = extractor.extract()

                self.log.debug(
                    f"Found task metadata for operation {task.task_id}: {task_metadata}"
                )
                if task_metadata:
                    return task_metadata

            except Exception as e:
                self.log.exception(
                    f"Failed to extract metadata {e} {task_info}",
                )
        else:
            self.log.warning(f"Unable to find an extractor. {task_info}")
            _inputs: List = []
            _outputs: List = []

            if collect_manual_lineage:
                self.log.warning(
                    "Inputs/outputs were defined manually and no extractor was found that excepts the given operator. Collecting manual lineage from the provided input and/or output definitions."
                )

                if len(task.get_inlet_defs()):
                    _inputs = list(
                        map(
                            self.extract_inlets_and_outlets,
                            task.get_inlet_defs(),
                        )
                    )
                    self.log.info(f"manual inputs: {_inputs}")
                if len(task.get_outlet_defs()):
                    _outputs = list(
                        map(
                            self.extract_inlets_and_outlets,
                            task.get_outlet_defs(),
                        )
                    )
                    self.log.info(f"manual outputs: {_outputs}")

            # Only include the unkonwnSourceAttribute facet if there is no extractor
            return TaskMetadata(
                name=get_job_name(task),
                run_facets={
                    "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                        unknownItems=[
                            UnknownOperatorInstance(
                                name=task.__class__.__name__,
                                properties={
                                    attr: value
                                    for attr, value in task.__dict__.items()
                                },
                            )
                        ]
                    )
                },
                inputs=_inputs,
                outputs=_outputs,
            )

        return TaskMetadata(name=get_job_name(task))

    def _get_extractor(self, task) -> Optional[BaseExtractor]:
        if task.task_id in self.extractors:
            return self.extractors[task.task_id]
        extractor = self.task_to_extractor.get_extractor_class(task.__class__)
        self.log.debug(f"extractor for {task.__class__} is {extractor}")
        if extractor:
            self.extractors[task.task_id] = extractor(task)
            return self.extractors[task.task_id]
        return None

    def extract_inlets_and_outlets(self, properties):
        return Dataset(
            namespace=properties["database"],
            name=properties["name"],
            facets=properties,
        )
