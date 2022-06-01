import logging
from typing import Optional, Type

from openlineage.airflow.extractors import TaskMetadata, BaseExtractor, Extractors
from openlineage.airflow.facets import UnknownOperatorAttributeRunFacet, UnknownOperatorInstance
from openlineage.airflow.utils import get_job_name, get_operator_class


class ExtractorManager:
    """Class abstracting management of custom extractors."""

    def __init__(self):
        self.extractors = {}
        self.task_to_extractor = Extractors()
        self.log = logging.getLogger()

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
                    return task_metadata

            except Exception as e:
                self.log.exception(
                    f'Failed to extract metadata {e} {task_info}',
                )
        else:
            self.log.warning(
                f'Unable to find an extractor. {task_info}')

            # Only include the unkonwnSourceAttribute facet if there is no extractor
            return TaskMetadata(
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

        return TaskMetadata(name=get_job_name(task))

    def _get_extractor(self, task) -> Optional[BaseExtractor]:
        if task.task_id in self.extractors:
            return self.extractors[task.task_id]
        extractor = self.task_to_extractor.get_extractor_class(get_operator_class(task))
        self.log.debug(f'extractor for {task.__class__} is {extractor}')
        if extractor:
            self.extractors[task.task_id] = extractor(task)
            return self.extractors[task.task_id]
        return None
