# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional, Type

from openlineage.airflow.extractors import BaseExtractor, Extractors, TaskMetadata
from openlineage.airflow.facets import UnknownOperatorAttributeRunFacet, UnknownOperatorInstance
from openlineage.airflow.utils import get_job_name, get_operator_class


class ExtractorManager:
    """Class abstracting management of custom extractors."""
    def __init__(self):
        self.extractors = {}
        self.task_to_extractor = Extractors()
        self.log = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    def add_extractor(self, operator, extractor: Type[BaseExtractor]):
        self.task_to_extractor.add_extractor(operator, extractor)

    def extract_metadata(
        self,
        dagrun,
        task,
        complete: bool = False,
        task_instance=None
    ) -> TaskMetadata:
        extractor = self._get_extractor(task)
        task_info = f'task_type={get_operator_class(task).__name__} ' \
            f'airflow_dag_id={task.dag_id} ' \
            f'task_id={task.task_id} ' \
            f'airflow_run_id={dagrun.run_id} '

        if extractor:
            # Extracting advanced metadata is only possible when extractor for particular operator
            # is defined. Without it, we can't extract any input or output data.
            try:
                self.log.debug(
                    f'Using extractor {extractor.__class__.__name__} {task_info}')
                if complete:
                    task_metadata = extractor.extract_on_complete(task_instance)
                else:
                    task_metadata = extractor.extract()

                self.log.debug(
                    f"Found task metadata for operation {task.task_id}: {task_metadata}"
                )
                if task_metadata:
                    if (not task_metadata.inputs) and (not task_metadata.outputs):
                        inlets = task.get_inlet_defs()
                        outlets = task.get_outlet_defs()
                        self.extract_inlets_and_outlets(task_metadata, inlets, outlets)

                    return task_metadata

            except Exception as e:
                self.log.exception(
                    f'Failed to extract metadata {e} {task_info}',
                )
        else:
            self.log.warning(
                f'Unable to find an extractor. {task_info}')

            # Only include the unkonwnSourceAttribute facet if there is no extractor
            task_metadata = TaskMetadata(
                name=get_job_name(task),
                run_facets={
                    "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                        unknownItems=[
                            UnknownOperatorInstance(
                                name=get_operator_class(task).__name__,
                                properties={
                                    attr: value for attr, value in task.__dict__.items()
                                },
                            )
                        ]
                    )
                },
            )
            inlets = task.get_inlet_defs()
            outlets = task.get_outlet_defs()
            self.extract_inlets_and_outlets(task_metadata, inlets, outlets)
            return task_metadata

        return TaskMetadata(name=get_job_name(task))

    def _get_extractor(self, task) -> Optional[BaseExtractor]:
        self.task_to_extractor.instantiate_abstract_extractors(task)
        if task.task_id in self.extractors:
            return self.extractors[task.task_id]
        extractor = self.task_to_extractor.get_extractor_class(get_operator_class(task))
        self.log.debug(f'extractor for {task.__class__} is {extractor}')
        if extractor:
            self.extractors[task.task_id] = extractor(task)
            return self.extractors[task.task_id]
        return None

    def extract_inlets_and_outlets(
            self,
            task_metadata: TaskMetadata,
            inlets: List,
            outlets: List,
    ):
        from openlineage.airflow.extractors.converters import convert_to_dataset

        self.log.debug("Manually extracting lineage metadata from inlets and outlets")
        for i in inlets:
            d = convert_to_dataset(i)
            if d:
                task_metadata.inputs.append(d)
        for o in outlets:
            d = convert_to_dataset(o)
            if d:
                task_metadata.outputs.append(d)
