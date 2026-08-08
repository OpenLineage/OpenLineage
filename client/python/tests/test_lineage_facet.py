# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from openlineage.client.facet_v2 import lineage
from openlineage.client.serde import Serde


def test_lineage_job_facet_serializes_dataset_and_job_variants() -> None:
    source_dataset = lineage.LineageDatasetInput(
        namespace="postgresql://warehouse",
        name="raw.orders",
        type=lineage.Type2.DATASET,
        field="customer_id",
        transformations=[lineage.LineageTransformation(type="DIRECT", subtype="IDENTITY")],
    )
    source_job = lineage.LineageJobInput(
        namespace="https://example.com/jobs",
        name="enrich_orders",
        type=lineage.Type3.JOB,
    )
    dataset_entry = lineage.LineageDatasetEntry(
        namespace="postgresql://warehouse",
        name="analytics.orders",
        type=lineage.Type.DATASET,
        inputs=[source_dataset, source_job],
        fields={"customer_id": lineage.LineageFieldEntry(inputs=[source_dataset])},
    )
    job_entry = lineage.LineageJobEntry(
        namespace="https://example.com/jobs",
        name="publish_orders",
        type=lineage.Type1.JOB,
        inputs=[source_dataset],
    )

    serialized = Serde.to_dict(
        lineage.LineageJobFacet(
            entries=[dataset_entry, job_entry],
            producer="https://example.com/lineage",
        )
    )

    assert serialized["_schemaURL"].endswith("LineageFacet.json#/$defs/LineageJobFacet")
    assert [entry["type"] for entry in serialized["entries"]] == ["DATASET", "JOB"]
    assert [source["type"] for source in serialized["entries"][0]["inputs"]] == ["DATASET", "JOB"]


def test_lineage_dataset_facet_preserves_explicit_empty_inputs() -> None:
    facet = lineage.LineageDatasetFacet(
        inputs=[],
        fields={
            "generated_at": lineage.LineageFieldEntry(
                inputs=[
                    lineage.LineageJobInput(
                        type=lineage.Type3.JOB,
                        transformations=[lineage.LineageTransformation(type="DIRECT", subtype="GENERATION")],
                    )
                ]
            )
        },
        producer="https://example.com/catalog",
    )

    serialized = Serde.to_dict(facet)

    assert serialized["_schemaURL"].endswith("LineageFacet.json#/$defs/LineageDatasetFacet")
    assert serialized["inputs"] == []
    assert serialized["fields"]["generated_at"]["inputs"][0]["type"] == "JOB"


def test_lineage_dataset_inputs_are_omitted_when_unspecified() -> None:
    source_job = lineage.LineageJobInput(type=lineage.Type3.JOB)
    fields = {"generated_at": lineage.LineageFieldEntry(inputs=[source_job])}

    dataset_facet = lineage.LineageDatasetFacet(
        fields=fields,
        producer="https://example.com/catalog",
    )
    dataset_entry = lineage.LineageDatasetEntry(
        namespace="postgresql://warehouse",
        name="analytics.generated",
        type=lineage.Type.DATASET,
        fields=fields,
    )
    dataset_entry_with_empty_inputs = lineage.LineageDatasetEntry(
        namespace="postgresql://warehouse",
        name="analytics.without_upstream",
        type=lineage.Type.DATASET,
        inputs=[],
    )

    serialized_facet = Serde.to_dict(dataset_facet)
    serialized_entries = Serde.to_dict(
        lineage.LineageJobFacet(
            entries=[dataset_entry, dataset_entry_with_empty_inputs],
            producer="https://example.com/lineage",
        )
    )["entries"]

    assert "inputs" not in serialized_facet
    assert "inputs" not in serialized_entries[0]
    assert serialized_entries[1]["inputs"] == []
