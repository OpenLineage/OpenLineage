# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from typing import Dict, List, Optional

import attr
import cattr
from openlineage.client.run import InputDataset, OutputDataset


@attr.s
class RunningStep:
    step_run_id: str = attr.ib()
    input_datasets: List[InputDataset] = attr.ib(factory=list)
    output_datasets: List[OutputDataset] = attr.ib(factory=list)


@attr.s
class RunningPipeline:
    running_steps: Dict[str, RunningStep] = attr.ib(factory=dict)
    repository_name: Optional[str] = attr.ib(default=None)


@attr.s
class OpenLineageCursor:
    last_storage_id: int = attr.ib()
    running_pipelines: Dict[str, RunningPipeline] = attr.ib(factory=dict)

    def to_json(self):
        return json.dumps(attr.asdict(self))

    @staticmethod
    def from_json(json_str: str):
        return cattr.structure(json.loads(json_str), OpenLineageCursor)
