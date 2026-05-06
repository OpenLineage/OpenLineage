# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage.client.facet_v2 import ownership_dataset
from openlineage.common.provider.dbt.processor import DbtArtifactProcessor, ModelNode


@pytest.fixture
def processor():
    proc = DbtArtifactProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="test-namespace",
    )
    proc.dataset_namespace = "duckdb://localhost"
    return proc


def _node(owner_value):
    metadata = {
        "unique_id": "model.proj.my_model",
        "database": "db",
        "schema": "sch",
        "alias": "my_model",
        "name": "my_model",
        "description": "",
        "columns": {},
        "meta": {} if owner_value is None else {"owner": owner_value},
    }
    return ModelNode(type="model", metadata_node=metadata, catalog_node=None)


def test_missing_owner_emits_no_facet(processor):
    _, _, facets, _ = processor.extract_dataset_data(_node(None), None, has_facets=True)
    assert "ownership" not in facets


def test_string_owner_emits_single(processor):
    _, _, facets, _ = processor.extract_dataset_data(_node("alice@example.com"), None, has_facets=True)
    assert facets["ownership"].owners == [ownership_dataset.Owner(name="alice@example.com")]


def test_list_owner_expands(processor):
    _, _, facets, _ = processor.extract_dataset_data(
        _node(["alice@example.com", "bob@example.com"]), None, has_facets=True
    )
    assert facets["ownership"].owners == [
        ownership_dataset.Owner(name="alice@example.com"),
        ownership_dataset.Owner(name="bob@example.com"),
    ]


def test_regression_single_element_list(processor):
    """Customer-reported payload: ['arkumar@rippling.com'] must produce a string-named Owner."""
    _, _, facets, _ = processor.extract_dataset_data(_node(["arkumar@rippling.com"]), None, has_facets=True)
    owners = facets["ownership"].owners
    assert owners == [ownership_dataset.Owner(name="arkumar@rippling.com")]
    assert isinstance(owners[0].name, str)
