# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy

from openlineage.airflow.facets import (
    AirflowMappedTaskRunFacet,
    AirflowRunArgsRunFacet,
    AirflowRunFacet,
    AirflowVersionRunFacet,
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from openlineage.client.serde import Serde


def test_facet_copy_serialization_airflow_version_run_facet():
    facet = AirflowVersionRunFacet(
        operator="operator",
        taskInfo={"task": "info"},
        airflowVersion="airflowVersion",
        openlineageAirflowVersion="openlineageAirflowVersion",
        producer="producer",
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_airflow_run_args_run_facet():
    facet = AirflowRunArgsRunFacet(externalTrigger=True, producer="producer")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_airflow_mapped_task_run_facet():
    facet = AirflowMappedTaskRunFacet(mapIndex=1, operatorClass="operatorClass", producer="producer")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_airflow_run_facet():
    facet = AirflowRunFacet(
        dag={"dag": ""},
        dagRun={"dagrun": ""},
        taskInstance={"taskInstance": ""},
        task={"task": ""},
        taskUuid="taskUuid",
        producer="producer",
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_unknown_operator_attribute_run_facet():
    facet = UnknownOperatorAttributeRunFacet(
        unknownItems=[UnknownOperatorInstance(name="name", properties={"properties": ""}, type="type")]
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)
