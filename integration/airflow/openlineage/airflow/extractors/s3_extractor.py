import logging
from typing import Optional, List
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.run import Dataset

log = logging.getLogger(__name__)


class S3CopyObjectExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['S3CopyObjectOperator']

    def extract(self) -> Optional[TaskMetadata]:

        input_object = Dataset(
            namespace="s3://{}".format(self.operator.source_bucket_name),
            name="s3://{0}/{1}".format(
                self.operator.source_bucket_name,
                self.operator.source_bucket_key
            ),
            facets={}
        )

        output_object = Dataset(
            namespace="s3://{}".format(self.operator.dest_bucket_name),
            name="s3://{0}/{1}".format(
                self.operator.dest_bucket_name,
                self.operator.dest_bucket_key
            ),
            facets={}
        )

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[input_object],
            outputs=[output_object],
        )

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        pass
