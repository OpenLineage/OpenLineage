# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from time import sleep
from typing import Union, Optional, List

from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.client.run import Dataset


class HangingExtractor(BaseExtractor):
    """
    Custom extractor that hangs for 30 seconds. The listener module should terminate the thread that executes
    this extractor after waiting for the timeout to complete.
    """

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["CustomOperator"]

    def extract(self) -> Union[Optional[TaskMetadata], List[TaskMetadata]]:
        sleep(30)
        return TaskMetadata(
            "test",
            inputs=[Dataset(namespace="test", name="dataset", facets={})],
        )
