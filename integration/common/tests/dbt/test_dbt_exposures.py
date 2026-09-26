# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage.client import set_producer
from openlineage.client.serde import Serde
from openlineage.common.provider.dbt.processor import DbtArtifactProcessor

PRODUCER = "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt"


@pytest.fixture(scope="session", autouse=True)
def setup_producer():
    set_producer(PRODUCER)


class _ConcreteDbtArtifactProcessor(DbtArtifactProcessor):
    """Minimal concrete subclass so tests can exercise parse_execution().

    ``dbt_run_run_facet`` and ``get_dbt_metadata`` are abstract on the base class (though
    unenforced, since it's not an ABC); provide trivial implementations.
    """

    def dbt_run_run_facet(self):
        return {}

    def get_dbt_metadata(self):
        raise NotImplementedError


def _processor(manifest: dict) -> DbtArtifactProcessor:
    processor = _ConcreteDbtArtifactProcessor(producer=PRODUCER, job_namespace="job-namespace")
    processor.dataset_namespace = "snowflake://account"
    processor.manifest = manifest
    return processor


def test_exposures_index_happy_path():
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "database": "mydb",
                "schema": "public",
                "name": "orders",
                "alias": "orders_v2",
            },
            "model.proj.customers": {
                "database": "mydb",
                "schema": "public",
                "name": "customers",
            },
        },
        "exposures": {
            "exposure.proj.growth_dashboard": {
                "unique_id": "exposure.proj.growth_dashboard",
                "name": "growth_dashboard",
                "type": "dashboard",
                "url": "https://bi.tool/1",
                "depends_on": {"nodes": ["model.proj.orders", "model.proj.customers"]},
            },
        },
    }

    index = _processor(manifest)._exposures_by_node_id

    assert set(index) == {"model.proj.orders", "model.proj.customers"}

    for node_id in ("model.proj.orders", "model.proj.customers"):
        exposures = index[node_id]
        assert len(exposures) == 1
        exposure = exposures[0]
        assert exposure.unique_id == "exposure.proj.growth_dashboard"
        assert exposure.name == "growth_dashboard"
        assert exposure.type == "dashboard"
        assert exposure.url == "https://bi.tool/1"


def test_exposures_index_indexes_source_dependencies_too():
    # An exposure can depend directly on a source(), whose id lives under
    # manifest["sources"] rather than manifest["nodes"]. We only need the raw node id
    # to index by -- source-backed exposures are out of scope for attachment (sources
    # only ever appear as inputs), but they must not break indexing of model deps.
    manifest = {
        "nodes": {
            "model.proj.orders": {"database": "mydb", "schema": "public", "name": "orders"},
        },
        "sources": {
            "source.proj.raw.events": {"database": "rawdb", "schema": "ingest", "name": "events"},
        },
        "exposures": {
            "exposure.proj.growth_dashboard": {
                "unique_id": "exposure.proj.growth_dashboard",
                "name": "growth_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["model.proj.orders", "source.proj.raw.events"]},
            },
        },
    }

    index = _processor(manifest)._exposures_by_node_id
    assert set(index) == {"model.proj.orders", "source.proj.raw.events"}
    assert index["model.proj.orders"][0].unique_id == "exposure.proj.growth_dashboard"
    assert index["source.proj.raw.events"][0].unique_id == "exposure.proj.growth_dashboard"


def test_exposures_index_missing_nodes_are_still_indexed_by_id():
    # An upstream node absent from the manifest (e.g. an ephemeral model) still gets
    # indexed by its raw id -- we no longer need to resolve it to manifest metadata.
    manifest = {
        "nodes": {},
        "exposures": {
            "exposure.proj.e": {
                "unique_id": "exposure.proj.e",
                "name": "e",
                "depends_on": {"nodes": ["model.proj.ephemeral"]},
            },
        },
    }

    index = _processor(manifest)._exposures_by_node_id
    assert set(index) == {"model.proj.ephemeral"}
    assert index["model.proj.ephemeral"][0].name == "e"


def test_exposures_index_missing_depends_on():
    manifest = {
        "nodes": {},
        "exposures": {
            "exposure.proj.e": {"unique_id": "exposure.proj.e", "name": "e"},
        },
    }

    index = _processor(manifest)._exposures_by_node_id
    assert index == {}


@pytest.mark.parametrize("manifest", [{"exposures": {}}, {"nodes": {}}, {}])
def test_exposures_index_no_exposures_returns_empty(manifest):
    assert _processor(manifest)._exposures_by_node_id == {}


def test_exposures_index_type_and_url_are_optional():
    manifest = {
        "exposures": {
            "exposure.proj.e": {
                "unique_id": "exposure.proj.e",
                "name": "e",
                "depends_on": {"nodes": ["model.proj.orders"]},
            },
        },
    }

    exposure = _processor(manifest)._exposures_by_node_id["model.proj.orders"][0]
    assert exposure.type is None
    assert exposure.url is None


def test_exposures_dataset_facet_serializes_to_expected_wire_shape():
    from openlineage.common.provider.dbt.facets import DbtExposure, DbtExposuresDatasetFacet

    facet = DbtExposuresDatasetFacet(
        exposures=[
            DbtExposure(
                unique_id="exposure.proj.e",
                name="e",
                type="dashboard",
            )
        ]
    )
    serialized = Serde.to_dict(facet)

    # _producer is always present; optional None fields (here: url) are dropped.
    assert serialized["_producer"] == PRODUCER
    assert serialized["exposures"] == [
        {
            "unique_id": "exposure.proj.e",
            "name": "e",
            "type": "dashboard",
        }
    ]


def test_exposures_attached_to_output_dataset_on_successful_model():
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "database": "mydb",
                "schema": "public",
                "name": "orders",
                "alias": "orders",
                "resource_type": "model",
                "columns": {},
            },
        },
        "parent_map": {"model.proj.orders": []},
        "exposures": {
            "exposure.proj.growth_dashboard": {
                "unique_id": "exposure.proj.growth_dashboard",
                "name": "growth_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["model.proj.orders"]},
            },
        },
    }

    context = _FakeRunContext(
        manifest=manifest,
        run_results={
            "results": [
                {
                    "unique_id": "model.proj.orders",
                    "status": "success",
                    "timing": [],
                    "adapter_response": {},
                }
            ]
        },
        catalog={},
    )

    processor = _processor(manifest)
    processor.manifest_version = 12
    events = processor.parse_execution(context, manifest["nodes"])

    complete_event = events.completes[0]
    output = complete_event.outputs[0]
    assert "dbt_exposures" in output.facets
    exposures = output.facets["dbt_exposures"].exposures
    assert len(exposures) == 1
    assert exposures[0].unique_id == "exposure.proj.growth_dashboard"


def test_exposures_not_attached_when_model_has_no_exposures():
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "database": "mydb",
                "schema": "public",
                "name": "orders",
                "alias": "orders",
                "resource_type": "model",
                "columns": {},
            },
        },
        "parent_map": {"model.proj.orders": []},
        "exposures": {},
    }

    context = _FakeRunContext(
        manifest=manifest,
        run_results={
            "results": [
                {
                    "unique_id": "model.proj.orders",
                    "status": "success",
                    "timing": [],
                    "adapter_response": {},
                }
            ]
        },
        catalog={},
    )

    processor = _processor(manifest)
    processor.manifest_version = 12
    events = processor.parse_execution(context, manifest["nodes"])

    complete_event = events.completes[0]
    output = complete_event.outputs[0]
    assert "dbt_exposures" not in output.facets


def test_source_backed_exposure_attached_to_input_dataset():
    # A source-backed exposure depends on a source() that a model reads. The source only
    # ever appears as an INPUT, so the facet must land on the input dataset (and NOT on the
    # output, which the source-backed exposure does not depend on).
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "database": "mydb",
                "schema": "public",
                "name": "orders",
                "alias": "orders",
                "resource_type": "model",
                "columns": {},
            },
        },
        "sources": {
            "source.proj.raw.events": {
                "database": "rawdb",
                "schema": "ingest",
                "name": "events",
                "resource_type": "source",
                "columns": {},
            },
        },
        "parent_map": {"model.proj.orders": ["source.proj.raw.events"]},
        "exposures": {
            "exposure.proj.source_dashboard": {
                "unique_id": "exposure.proj.source_dashboard",
                "name": "source_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["source.proj.raw.events"]},
            },
        },
    }

    context = _FakeRunContext(
        manifest=manifest,
        run_results={
            "results": [
                {
                    "unique_id": "model.proj.orders",
                    "status": "success",
                    "timing": [],
                    "adapter_response": {},
                }
            ]
        },
        catalog={},
    )

    processor = _processor(manifest)
    processor.manifest_version = 12
    events = processor.parse_execution(context, manifest["nodes"])

    complete_event = events.completes[0]

    # The source-backed exposure lands on the input dataset.
    assert len(complete_event.inputs) == 1
    input_dataset = complete_event.inputs[0]
    assert "dbt_exposures" in input_dataset.facets
    input_exposures = input_dataset.facets["dbt_exposures"].exposures
    assert [e.unique_id for e in input_exposures] == ["exposure.proj.source_dashboard"]

    # The output dataset carries no exposures (nothing depends on the model itself).
    assert "dbt_exposures" not in complete_event.outputs[0].facets


def test_model_and_source_backed_exposures_split_across_output_and_input():
    # A model-backed exposure lands on the output; a source-backed exposure lands on the
    # input -- both for the same successful model build.
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "database": "mydb",
                "schema": "public",
                "name": "orders",
                "alias": "orders",
                "resource_type": "model",
                "columns": {},
            },
        },
        "sources": {
            "source.proj.raw.events": {
                "database": "rawdb",
                "schema": "ingest",
                "name": "events",
                "resource_type": "source",
                "columns": {},
            },
        },
        "parent_map": {"model.proj.orders": ["source.proj.raw.events"]},
        "exposures": {
            "exposure.proj.model_dashboard": {
                "unique_id": "exposure.proj.model_dashboard",
                "name": "model_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["model.proj.orders"]},
            },
            "exposure.proj.source_dashboard": {
                "unique_id": "exposure.proj.source_dashboard",
                "name": "source_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["source.proj.raw.events"]},
            },
        },
    }

    context = _FakeRunContext(
        manifest=manifest,
        run_results={
            "results": [
                {
                    "unique_id": "model.proj.orders",
                    "status": "success",
                    "timing": [],
                    "adapter_response": {},
                }
            ]
        },
        catalog={},
    )

    processor = _processor(manifest)
    processor.manifest_version = 12
    events = processor.parse_execution(context, manifest["nodes"])

    complete_event = events.completes[0]

    output_exposures = complete_event.outputs[0].facets["dbt_exposures"].exposures
    assert [e.unique_id for e in output_exposures] == ["exposure.proj.model_dashboard"]

    input_exposures = complete_event.inputs[0].facets["dbt_exposures"].exposures
    assert [e.unique_id for e in input_exposures] == ["exposure.proj.source_dashboard"]


def test_source_backed_exposure_not_attached_on_failed_model():
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "database": "mydb",
                "schema": "public",
                "name": "orders",
                "alias": "orders",
                "resource_type": "model",
                "columns": {},
            },
        },
        "sources": {
            "source.proj.raw.events": {
                "database": "rawdb",
                "schema": "ingest",
                "name": "events",
                "resource_type": "source",
                "columns": {},
            },
        },
        "parent_map": {"model.proj.orders": ["source.proj.raw.events"]},
        "exposures": {
            "exposure.proj.source_dashboard": {
                "unique_id": "exposure.proj.source_dashboard",
                "name": "source_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["source.proj.raw.events"]},
            },
        },
    }

    context = _FakeRunContext(
        manifest=manifest,
        run_results={
            "results": [
                {
                    "unique_id": "model.proj.orders",
                    "status": "error",
                    "timing": [],
                    "adapter_response": {},
                }
            ]
        },
        catalog={},
    )

    processor = _processor(manifest)
    processor.manifest_version = 12
    events = processor.parse_execution(context, manifest["nodes"])

    fail_event = events.fails[0]
    assert "dbt_exposures" not in fail_event.inputs[0].facets


class _FakeRunContext:
    def __init__(self, manifest, run_results, catalog):
        self.manifest = manifest
        self.run_results = run_results
        self.catalog = catalog
