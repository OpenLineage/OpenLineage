# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.event_v2 import Dataset


class GCSToGCSExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["GCSToGCSOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        if self.operator.source_object:
            input_objects = [
                Dataset(
                    namespace=f"gs://{self.operator.source_bucket}",
                    name=f"gs://{self.operator.source_bucket}/{self.operator.source_object}",
                    facets={},
                )
            ]
        else:
            input_objects = [
                Dataset(
                    namespace=f"gs://{self.operator.source_bucket}",
                    name=f"gs://{self.operator.source_bucket}/{source_object}",
                    facets={},
                )
                for source_object in self.operator.source_objects
            ]

        output_object = Dataset(
            namespace=f"gs://{self.operator.destination_bucket}",
            name=f"gs://{self.operator.destination_bucket}/{self.operator.destination_object}",
            facets={},
        )

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=input_objects,
            outputs=[output_object],
        )
