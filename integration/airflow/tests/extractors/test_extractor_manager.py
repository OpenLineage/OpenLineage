# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Optional, List
from unittest.mock import MagicMock

from airflow.models import BaseOperator
from airflow.lineage.entities import Table

from openlineage.airflow.extractors import ExtractorManager, BaseExtractor, TaskMetadata
from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor
from openlineage.client.run import Dataset


class FakeOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(FakeOperator, self).__init__(*args, **kwargs)
        self.executed = False

    def execute(self, context: Any):
        self.executed = True


class FakeExtractor(BaseExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["FakeOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        return TaskMetadata(name="fake-name", job_facets={
            "fake": {"executed": self.operator.executed}
        })


def test_fake_extractor_extracts():
    dagrun = MagicMock()
    task = FakeOperator(task_id="task")

    manager = ExtractorManager()
    manager.add_extractor(FakeOperator.__name__, FakeExtractor)
    metadata = manager.extract_metadata(dagrun, task)

    assert len(metadata.job_facets) == 1
    assert metadata.job_facets["fake"]["executed"] is False


def test_no_extractor_proper_name():
    dagrun = MagicMock()
    task = MagicMock()
    task.dag_id = "a"
    task.task_id = "b"

    extractors = ExtractorManager()
    metadata = extractors.extract_metadata(dagrun, task)

    assert metadata.name == "a.b"


def test_adding_extractors_to_manager():
    manager = ExtractorManager()
    count = len(manager.task_to_extractor.extractors)
    manager.add_extractor("test", PostgresExtractor)
    assert len(manager.task_to_extractor.extractors) == count + 1


def test_extracting_inlets_and_outlets():
    metadata = TaskMetadata(name="fake-name", job_facets={})
    inlets=[Table(database="d1", cluster="c1", name="t1")]
    outlets=[Table(database="d1", cluster="c1", name="t2")]

    manager = ExtractorManager()
    manager.extract_inlets_and_outlets(metadata, inlets, outlets)

    assert len(metadata.inputs) == 1 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)

def test_extraction_from_inlets_and_outlets_without_extractor():
    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[Table(database="d1", cluster="c1", name="t1")],
        outlets=[Table(database="d1", cluster="c1", name="t2")],
    )

    manager = ExtractorManager()

    metadata = manager.extract_metadata(dagrun, task)
    assert len(metadata.inputs) == 1 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)

def test_fake_extractor_extracts_from_inlets_and_outlets():
    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[Table(database="d1", cluster="c1", name="t1")],
        outlets=[Table(database="d1", cluster="c1", name="t2")],
    )

    manager = ExtractorManager()
    manager.add_extractor(FakeOperator.__name__, FakeExtractor)

    metadata = manager.extract_metadata(dagrun, task)
    assert len(metadata.inputs) == 1 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)

