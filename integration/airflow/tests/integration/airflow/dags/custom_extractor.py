from typing import Union, Optional, List

from openlineage.client.run import Dataset
from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor


class BashExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BashOperator']

    def extract(self) -> Union[Optional[TaskMetadata], List[TaskMetadata]]:
        return TaskMetadata(
            "test",
            inputs=[
                Dataset(
                    namespace="test",
                    name="dataset",
                    facets={}
                )
            ]
        )
