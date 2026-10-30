# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import json

from openlineage.client.facet_v2 import job_type_job, parent_job
from openlineage.client.serde import Serde


def test_parent_job_facet_minimal_serialization() -> None:
    facet = parent_job.ParentJobFacet(job=parent_job.Job(namespace="airflow", name="my_dag.my_task"))

    serialized = json.loads(Serde.to_json(facet))

    assert serialized["_schemaURL"] == (
        "https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json#/$defs/ParentJobFacet"
    )
    assert serialized["job"]["namespace"] == "airflow"
    assert serialized["job"]["name"] == "my_dag.my_task"
    assert "root" not in serialized
    assert "run" not in serialized


def test_parent_job_facet_with_root_and_forwarded_facets() -> None:
    task_type = job_type_job.JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="TASK")
    dag_type = job_type_job.JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="DAG")
    facet = parent_job.ParentJobFacet(
        job=parent_job.Job(namespace="airflow", name="my_dag.my_task", facets={"jobType": task_type}),
        root=parent_job.Root(
            job=parent_job.RootJobIdentifier(namespace="airflow", name="my_dag", facets={"jobType": dag_type})
        ),
    )

    serialized = json.loads(Serde.to_json(facet))

    assert serialized["job"]["name"] == "my_dag.my_task"
    assert serialized["job"]["facets"]["jobType"]["jobType"] == "TASK"
    assert serialized["root"]["job"]["name"] == "my_dag"
    assert serialized["root"]["job"]["facets"]["jobType"]["jobType"] == "DAG"
    assert "run" not in serialized
    assert "run" not in serialized["root"]


def test_parent_job_facet_copy_serialization() -> None:
    facet = parent_job.ParentJobFacet(
        job=parent_job.Job(namespace="default", name="parent_job_name"),
        root=parent_job.Root(job=parent_job.RootJobIdentifier(namespace="root_ns", name="root_job_name")),
    )

    assert Serde.to_json(facet) == Serde.to_json(copy.deepcopy(facet))
