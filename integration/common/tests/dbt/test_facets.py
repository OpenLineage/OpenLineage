# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy

from openlineage.client.serde import Serde
from openlineage.common.provider.dbt.facets import DbtNodeJobFacet, DbtRunRunFacet, DbtVersionRunFacet


def test_facet_copy_serialization_dbt_version_run_facet():
    facet = DbtVersionRunFacet(version="version", producer="producer")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_dbt_run_run_facet():
    facet = DbtRunRunFacet(
        invocation_id="invocation_id", project_name="project_name", dbt_runtime="core", producer="producer"
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_dbt_node_job_facet():
    facet = DbtNodeJobFacet(
        original_file_path="models/staging/stg_customers.sql",
        database="analytics",
        schema="staging",
        alias="stg_customers",
        unique_id="model.my_project.stg_customers",
        producer="producer",
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)
