# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage.client import set_producer
from openlineage.client.facet_v2 import tags_run
from openlineage.common.provider.dbt.processor import (
    DbtArtifactProcessor,
    DbtRunContext,
)


@pytest.fixture(scope="session", autouse=True)
def setup_producer():
    set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt")


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("sometag", tags_run.TagsRunFacetFields(key="sometag", value="true", source="DBT")),
        ("team:analytics", tags_run.TagsRunFacetFields(key="team", value="analytics", source="DBT")),
        ("env:production:INFRA", tags_run.TagsRunFacetFields(key="env", value="production", source="INFRA")),
        ("pii:", tags_run.TagsRunFacetFields(key="pii", value="", source="DBT")),
        ("a:b:c:d", tags_run.TagsRunFacetFields(key="a:b:c:d", value="true", source="DBT")),
    ],
)
def test_parse_dbt_tag(tag, expected):
    assert DbtArtifactProcessor._parse_dbt_tag(tag) == expected


def test_build_dbt_tags_facet():
    processor = DbtArtifactProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="job-namespace",
    )

    facet = processor._dbt_tags_facet(["team:analytics", "environment:production:INFRA", "legacy-tag"])

    assert facet.tags == [
        tags_run.TagsRunFacetFields(key="team", value="analytics", source="DBT"),
        tags_run.TagsRunFacetFields(key="environment", value="production", source="INFRA"),
        tags_run.TagsRunFacetFields(key="legacy-tag", value="true", source="DBT"),
    ]


def test_seed_snapshot_nodes_do_not_throw():
    processor = DbtArtifactProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="job-namespace",
    )

    # Should just skip processing
    processor.parse_assertions(
        DbtRunContext({}, {"results": [{"unique_id": "seed.jaffle_shop.raw_orders"}]}),
        {},
    )
