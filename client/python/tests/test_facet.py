# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import json
from typing import Any

import pytest
from openlineage.client.client import OpenLineageClient
from openlineage.client.facet import (
    BaseFacet,
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    DatasetVersionDatasetFacet,
    JobTypeJobFacet,
    LifecycleStateChange,
    LifecycleStateChangeDatasetFacet,
    LifecycleStateChangeDatasetFacetPreviousIdentifier,
    OwnershipDatasetFacet,
    OwnershipDatasetFacetOwners,
    OwnershipJobFacet,
    OwnershipJobFacetOwners,
    StorageDatasetFacet,
    SymlinksDatasetFacet,
    SymlinksDatasetFacetIdentifiers,
)
from openlineage.client.run import SCHEMA_URL, Dataset, Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde


@pytest.fixture
def event() -> dict[str, Any]:
    return {
        "eventType": "START",
        "eventTime": "2021-11-03T10:53:52.427343",
        "job": {
            "namespace": "openlineage",
            "name": "job",
            "facets": {},
        },
        "run": {
            "runId": "69f4acab-b87d-4fc0-b27b-8ea950370ff3",
            "facets": {},
        },
        "inputs": [],
        "outputs": [
            {
                "namespace": "some-namespace",
                "name": "input-dataset",
                "facets": {},
            },
        ],
        "producer": "some-producer",
        "schemaURL": SCHEMA_URL,
    }


def test_symlink_dataset_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    symlink_facet = {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
        "/OpenLineage.json#/definitions/SymlinksDatasetFacet",
        "identifiers": [
            {
                "namespace": "symlink-namespace",
                "name": "symlink-name",
                "type": "TABLE",
            },
        ],
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "symlinks": SymlinksDatasetFacet(
                            identifiers=[
                                SymlinksDatasetFacetIdentifiers(
                                    namespace="symlink-namespace",
                                    name="symlink-name",
                                    type="TABLE",
                                ),
                            ],
                        ),
                    },
                ),
            ],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    expected_event = copy.deepcopy(event)
    expected_event["outputs"][0]["facets"] = {}
    expected_event["outputs"][0]["facets"]["symlinks"] = symlink_facet

    assert expected_event == event_sent


def test_storage_dataset_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    storage_facet = {
        "storageLayer": "iceberg",
        "fileFormat": "parquet",
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
        "/OpenLineage.json#/definitions/StorageDatasetFacet",
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "storage": StorageDatasetFacet(
                            storageLayer="iceberg",
                            fileFormat="parquet",
                        ),
                    },
                ),
            ],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    expected_event = copy.deepcopy(event)
    expected_event["outputs"][0]["facets"] = {}
    expected_event["outputs"][0]["facets"]["storage"] = storage_facet

    assert expected_event == event_sent


def test_ownership_job_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    ownership_job_facet = {
        "owners": [
            {
                "name": "some-owner",
                "type": "some-owner-type",
            },
        ],
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
        "/OpenLineage.json#/definitions/OwnershipJobFacet",
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job(
                "openlineage",
                "job",
                {
                    "ownership": OwnershipJobFacet(
                        owners=[
                            OwnershipJobFacetOwners("some-owner", "some-owner-type"),
                        ],
                    ),
                },
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                ),
            ],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    expected_event = copy.deepcopy(event)
    expected_event["job"]["facets"] = {}
    expected_event["job"]["facets"]["ownership"] = ownership_job_facet

    assert expected_event == event_sent


def test_dataset_version_dataset_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    dataset_version_facet = {
        "datasetVersion": "v0.1",
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
        "/OpenLineage.json#/definitions/DatasetVersionDatasetFacet",
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job(
                "openlineage",
                "job",
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "version": DatasetVersionDatasetFacet(
                            datasetVersion="v0.1",
                        ),
                    },
                ),
            ],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    expected_event = copy.deepcopy(event)
    expected_event["outputs"][0]["facets"] = {}
    expected_event["outputs"][0]["facets"]["version"] = dataset_version_facet

    assert expected_event == event_sent


def test_lifecycle_state_change_dataset_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    lifecycle_state_change_dataset_facet = {
        "lifecycleStateChange": "DROP",
        "previousIdentifier": {
            "namespace": "previous-namespace",
            "name": "previous-name",
        },
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
        "/OpenLineage.json#/definitions/LifecycleStateChangeDatasetFacet",
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job(
                "openlineage",
                "job",
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                            lifecycleStateChange=LifecycleStateChange.DROP,
                            previousIdentifier=LifecycleStateChangeDatasetFacetPreviousIdentifier(
                                namespace="previous-namespace",
                                name="previous-name",
                            ),
                        ),
                    },
                ),
            ],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    dataset_facets = {}
    dataset_facets["lifecycleStateChange"] = lifecycle_state_change_dataset_facet
    expected_event = copy.deepcopy(event)
    expected_event["outputs"][0]["facets"] = dataset_facets

    assert expected_event == event_sent


def test_ownership_dataset_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    ownership_dataset_facet = {
        "owners": [
            {
                "name": "some-owner",
                "type": "some-owner-type",
            },
        ],
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
        "/OpenLineage.json#/definitions/OwnershipDatasetFacet",
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job(
                "openlineage",
                "job",
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "ownership": OwnershipDatasetFacet(
                            owners=[
                                OwnershipDatasetFacetOwners(
                                    name="some-owner",
                                    type="some-owner-type",
                                ),
                            ],
                        ),
                    },
                ),
            ],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    expected_event = copy.deepcopy(event)
    expected_event["outputs"][0]["facets"] = {}
    expected_event["outputs"][0]["facets"]["ownership"] = ownership_dataset_facet

    assert expected_event == event_sent


def test_column_lineage_dataset_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    column_lineage_dataset_facet = {
        "fields": {
            "output-field": {
                "inputFields": [
                    {
                        "namespace": "namespace-of-input-field-dataset",
                        "name": "name-of-input-field-dataset",
                        "field": "some-field-name",
                    },
                ],
                "transformationDescription": "some-transformation",
                "transformationType": "some-transformation-type",
            },
        },
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
        "/OpenLineage.json#/definitions/ColumnLineageDatasetFacet",
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job(
                "openlineage",
                "job",
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "columnLineage": ColumnLineageDatasetFacet(
                            {
                                "output-field": ColumnLineageDatasetFacetFieldsAdditional(
                                    transformationDescription="some-transformation",
                                    transformationType="some-transformation-type",
                                    inputFields=[
                                        ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                            namespace="namespace-of-input-field-dataset",
                                            name="name-of-input-field-dataset",
                                            field="some-field-name",
                                        ),
                                    ],
                                ),
                            },
                        ),
                    },
                ),
            ],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    expected_event = copy.deepcopy(event)
    expected_event["outputs"][0]["facets"] = {}
    expected_event["outputs"][0]["facets"]["columnLineage"] = column_lineage_dataset_facet

    assert expected_event == event_sent


def test_job_type_job_facet(mock_http_session_class, event: dict[str, Any]) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

    job_type_facet = {
        "processingType": "BATCH",
        "integration": "SPARK",
        "jobType": "QUERY",
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/JobTypeJobFacet",
    }

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job(
                namespace="openlineage",
                name="job",
                facets={"jobType": JobTypeJobFacet("BATCH", "SPARK", "QUERY")},
            ),
            "some-producer",
            [],
            [],
        ),
    )

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    event_sent = json.loads(call_args.kwargs["data"])

    expected_event = copy.deepcopy(event)
    expected_event["outputs"] = []
    expected_event["job"]["facets"] = {}
    expected_event["job"]["facets"]["jobType"] = job_type_facet

    assert expected_event == event_sent


def test_facet_copy_serialization_base_facet():
    facet = BaseFacet()
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_parent_run_facet():
    facet = OwnershipJobFacet(
        owners=[
            OwnershipJobFacetOwners("some-owner", "some-owner-type"),
        ],
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)
