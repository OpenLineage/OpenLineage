# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy

from openlineage.client.serde import Serde
from openlineage.common.provider.dbt.facets import DbtRunRunFacet, DbtVersionRunFacet


def test_facet_copy_serialization_dbt_version_run_facet():
    facet = DbtVersionRunFacet(version="version", producer="producer")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_dbt_run_run_facet():
    facet = DbtRunRunFacet(invocation_id="invocation_id", producer="producer")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)
