# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional
from urllib.parse import urlparse

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.event_v2 import Dataset


class S3CopyObjectExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["S3CopyObjectOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        input_object = Dataset(
            namespace=f"s3://{self.operator.source_bucket_name}",
            name=f"s3://{self.operator.source_bucket_name}/{self.operator.source_bucket_key}",
            facets={},
        )

        output_object = Dataset(
            namespace=f"s3://{self.operator.dest_bucket_name}",
            name=f"s3://{self.operator.dest_bucket_name}/{self.operator.dest_bucket_key}",
            facets={},
        )

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[input_object],
            outputs=[output_object],
        )


class S3FileTransformExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["S3FileTransformOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        input_object = Dataset(
            namespace=f"s3://{urlparse(self.operator.source_s3_key).netloc}",
            name=self.operator.source_s3_key,
            facets={},
        )

        output_object = Dataset(
            namespace=f"s3://{urlparse(self.operator.dest_s3_key).netloc}",
            name=self.operator.dest_s3_key,
            facets={},
        )

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[input_object],
            outputs=[output_object],
        )

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        pass
