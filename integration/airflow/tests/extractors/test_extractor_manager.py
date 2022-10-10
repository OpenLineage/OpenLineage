# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Optional, List
from unittest.mock import MagicMock
from airflow.models import BaseOperator

from openlineage.airflow.extractors import ExtractorManager, BaseExtractor, TaskMetadata
from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor


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

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        from openlineage.client.run import Dataset

        return TaskMetadata(
            name="fake-name",
            job_facets={"fake": {"executed": self.operator.executed}},
            inputs=[Dataset(namespace="example", name="ip_table")],
            outputs=[Dataset(namespace="example", name="op_table")]
        )


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
    from airflow.lineage.entities import Table
    from openlineage.client.run import Dataset

    metadata = TaskMetadata(name="fake-name", job_facets={})
    inlets = [Dataset(namespace="c1", name="d1.t0", facets={}),
              Table(database="d1", cluster="c1", name="t1")]
    outlets = [Table(database="d1", cluster="c1", name="t2")]

    manager = ExtractorManager()
    manager.extract_inlets_and_outlets(metadata, inlets, outlets)

    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.inputs[1], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)


def test_extraction_from_inlets_and_outlets_without_extractor():
    from airflow.lineage.entities import Table
    from openlineage.client.run import Dataset

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[Dataset(namespace="c1", name="d1.t0", facets={}),
                Table(database="d1", cluster="c1", name="t1")],
        outlets=[Table(database="d1", cluster="c1", name="t2")],
    )

    manager = ExtractorManager()

    metadata = manager.extract_metadata(dagrun, task)
    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.inputs[1], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)


def test_extraction_from_inlets_and_outlets_ignores_unhandled_types():
    from airflow.lineage.entities import Table, File
    from openlineage.client.run import Dataset

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[Dataset(namespace="c1", name="d1.t0", facets={}),
                File(url="http://test"), Table(database="d1", cluster="c1", name="t1")],
        outlets=[Table(database="d1", cluster="c1", name="t2"), File(url="http://test")],
    )

    manager = ExtractorManager()

    metadata = manager.extract_metadata(dagrun, task)
    # The File objects from inlets and outlets should not be converted
    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 1


def test_fake_extractor_extracts_from_inlets_and_outlets():
    from airflow.lineage.entities import Table
    from openlineage.client.run import Dataset

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[Dataset(namespace="c1", name="d1.t0", facets={}),
                Table(database="d1", cluster="c1", name="t1")],
        outlets=[Table(database="d1", cluster="c1", name="t2"),
                 Dataset(namespace="c1", name="d1.t3", facets={})],
    )

    manager = ExtractorManager()
    manager.add_extractor(FakeOperator.__name__, FakeExtractor)

    metadata = manager.extract_metadata(dagrun, task)
    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 2
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.inputs[1], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)
    assert isinstance(metadata.outputs[1], Dataset)
    assert metadata.inputs[0].name == "d1.t0"
    assert metadata.inputs[1].name == "d1.t1"
    assert metadata.outputs[0].name == "d1.t2"
    assert metadata.outputs[1].name == "d1.t3"


def test_fake_extractor_extracts_and_discards_inlets_and_outlets():
    from airflow.lineage.entities import Table
    from openlineage.client.run import Dataset

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[Dataset(namespace="c1", name="d1.t0", facets={}),
                Table(database="d1", cluster="c1", name="t1")],
        outlets=[Table(database="d1", cluster="c1", name="t2")],
    )

    manager = ExtractorManager()
    manager.add_extractor(FakeOperator.__name__, FakeExtractor)

    metadata = manager.extract_metadata(dagrun, task, complete=True)
    assert len(metadata.inputs) == 1 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)
    assert metadata.inputs[0].name == "ip_table"
    assert metadata.outputs[0].name == "op_table"
