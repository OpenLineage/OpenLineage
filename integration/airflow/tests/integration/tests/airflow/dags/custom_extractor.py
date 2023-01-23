# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Union

from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.client.run import Dataset


class CustomExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['CustomOperator']

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
