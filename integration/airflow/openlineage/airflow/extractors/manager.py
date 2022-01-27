import logging
from typing import Optional

from openlineage.airflow.extractors import TaskMetadata, BaseExtractor, Extractors
from openlineage.airflow.utils import get_job_name


class ExtractorManager:
    def __init__(self):
        self.extractors = {}
        self.extractor_mapper = Extractors()
        self.log = logging.getLogger()

    def extract_metadata(self, dagrun, task, complete: bool, task_instance=None) -> TaskMetadata:
        extractor = self._get_extractor(task)
        task_info = f'task_type={task.__class__.__name__} ' \
            f'airflow_dag_id={task.dag_id} ' \
            f'task_id={task.task_id} ' \
            f'airflow_run_id={dagrun.run_id} '
        if extractor:
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

        return TaskMetadata(
            name=get_job_name(task)
        )

    def _get_extractor(self, task) -> Optional[BaseExtractor]:
        if task.task_id in self.extractors:
            return self.extractors[task.task_id]
        extractor = self.extractor_mapper.get_extractor_class(task.__class__)
        self.log.debug(f'extractor for {task.__class__} is {extractor}')
        if extractor:
            self.extractors[task.task_id] = extractor(task)
            return self.extractors[task.task_id]
        return None
