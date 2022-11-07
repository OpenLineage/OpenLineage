# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from typing import Any

from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from unittest import mock
import attr

from openlineage.airflow.extractors import Extractors
from openlineage.airflow.extractors.base import (
    OperatorLineage,
    DefaultExtractor,
    TaskMetadata,
)
from openlineage.airflow.extractors.python_extractor import PythonExtractor
from openlineage.client.facet import ParentRunFacet, SqlJobFacet, BaseFacet
from openlineage.client.run import Dataset


INPUTS = [Dataset(namespace="database://host:port", name="inputtable")]
OUTPUTS = [Dataset(namespace="database://host:port", name="inputtable")]
RUN_FACETS = {
    "parent": ParentRunFacet.create(
        "3bb703d1-09c1-4a42-8da5-35a0b3216072", "namespace", "parentjob"
    )
}
JOB_FACETS = {"sql": SqlJobFacet(query="SELECT * FROM inputtable")}


@attr.s
class CompleteRunFacet(BaseFacet):
    finished: bool = attr.ib(default=False)


FINISHED_FACETS = {"complete": CompleteRunFacet(True)}


class ExampleOperator(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=JOB_FACETS,
        )

    def get_openlineage_facets_on_complete(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=FINISHED_FACETS,
        )


class OperatorWihoutComplete(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=JOB_FACETS,
        )


class OperatorWihoutStart(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_complete(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=FINISHED_FACETS,
        )


class BrokenOperator(BaseOperator):
    get_openlineage_facets = []

    def execute(self, context) -> Any:
        pass


def test_default_extraction():
    extractor = Extractors().get_extractor_class(ExampleOperator)
    assert extractor is DefaultExtractor

    metadata = extractor(ExampleOperator(task_id="test")).extract()

    task_instance = mock.MagicMock()

    metadata_on_complete = extractor(
        ExampleOperator(task_id="test")
    ).extract_on_complete(task_instance=task_instance)

    assert metadata == TaskMetadata(
        name="adhoc_airflow.test",
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=JOB_FACETS,
    )

    assert metadata_on_complete == TaskMetadata(
        name="adhoc_airflow.test",
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=FINISHED_FACETS,
    )


def test_extraction_without_on_complete():
    extractor = Extractors().get_extractor_class(OperatorWihoutComplete)
    assert extractor is DefaultExtractor

    metadata = extractor(OperatorWihoutComplete(task_id="test")).extract()

    task_instance = mock.MagicMock()

    metadata_on_complete = extractor(
        OperatorWihoutComplete(task_id="test")
    ).extract_on_complete(task_instance=task_instance)

    expected_task_metadata = TaskMetadata(
        name="adhoc_airflow.test",
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=JOB_FACETS,
    )

    assert metadata == expected_task_metadata

    assert metadata_on_complete == expected_task_metadata


def test_extraction_without_on_start():
    extractor = Extractors().get_extractor_class(OperatorWihoutStart)
    assert extractor is DefaultExtractor

    metadata = extractor(OperatorWihoutStart(task_id="test")).extract()

    task_instance = mock.MagicMock()

    metadata_on_complete = extractor(
        OperatorWihoutStart(task_id="test")
    ).extract_on_complete(task_instance=task_instance)

    assert metadata is None

    assert metadata_on_complete == TaskMetadata(
        name="adhoc_airflow.test",
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=FINISHED_FACETS,
    )


def test_does_not_use_default_extractor_when_not_a_method():
    extractor = Extractors().get_extractor_class(BrokenOperator)
    assert extractor is None


def test_does_not_use_default_extractor_when_no_get_openlineage_facets():
    extractor = Extractors().get_extractor_class(BaseOperator)
    assert extractor is None


def test_does_not_use_default_extractor_when_explicite_extractor():
    extractor = Extractors().get_extractor_class(PythonOperator)
    assert extractor is PythonExtractor
