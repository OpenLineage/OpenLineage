# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import copy

from unittest.mock import MagicMock

from openlineage.client.client import OpenLineageClient
from openlineage.client.facet import SymlinksDatasetFacet, SymlinksDatasetFacetIdentifiers, \
    StorageDatasetFacet, OwnershipJobFacet, OwnershipJobFacetOwners, DatasetVersionDatasetFacet, \
    LifecycleStateChangeDatasetFacet, LifecycleStateChangeDatasetFacetPreviousIdentifier, \
    LifecycleStateChange, OwnershipDatasetFacetOwners, OwnershipDatasetFacet, \
    ColumnLineageDatasetFacet, ColumnLineageDatasetFacetFieldsAdditional, \
    ColumnLineageDatasetFacetFieldsAdditionalInputFields
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

openlineage_event = {
    "eventType": "START",
    "eventTime": "2021-11-03T10:53:52.427343",
    "job": {
        "namespace": "openlineage",
        "name": "job",
        "facets": {}
    },
    "run": {
        "runId": "69f4acab-b87d-4fc0-b27b-8ea950370ff3",
        "facets": {}
    },
    "inputs": [
    ],
    "outputs": [
        {
            "namespace": "some-namespace",
            "name": "input-dataset",
            "facets": {}
        }
    ],
    "producer": "some-producer"
}


def test_symlink_dataset_facet():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    symlink_facet = {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec"
                      "/OpenLineage.json#/definitions/SymlinksDatasetFacet",
        "identifiers": [
            {
                "namespace": "symlink-namespace",
                "name": "symlink-name",
                "type": "TABLE"
            }
        ]
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
                                    type="TABLE"
                                )
                            ]
                        )
                    }
                )
            ]
        )
    )

    event_sent = json.loads(session.post.call_args[0][1])

    expected_event = copy.deepcopy(openlineage_event)
    expected_event["outputs"][0]["facets"] = dict()
    expected_event["outputs"][0]["facets"]["symlinks"] = symlink_facet

    assert expected_event == event_sent


def test_storage_dataset_facet():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

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
                            fileFormat="parquet"
                        )
                    }
                )
            ]
        )
    )

    event_sent = json.loads(session.post.call_args[0][1])

    expected_event = copy.deepcopy(openlineage_event)
    expected_event["outputs"][0]["facets"] = dict()
    expected_event["outputs"][0]["facets"]["storage"] = storage_facet

    assert expected_event == event_sent


def test_ownership_job_facet():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    ownership_job_facet = {
        "owners": [
            {
                "name": "some-owner",
                "type": "some-owner-type"
            }
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
                            OwnershipJobFacetOwners("some-owner", "some-owner-type")
                        ]
                    )
                }
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset"
                )
            ]
        )
    )

    event_sent = json.loads(session.post.call_args[0][1])

    expected_event = copy.deepcopy(openlineage_event)
    expected_event["job"]["facets"] = dict()
    expected_event["job"]["facets"]["ownership"] = ownership_job_facet

    assert expected_event == event_sent


def test_dataset_version_dataset_facet():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

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
                "job"
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "version": DatasetVersionDatasetFacet(
                            datasetVersion="v0.1"
                        )
                    }
                )
            ]
        )
    )

    event_sent = json.loads(session.post.call_args[0][1])

    expected_event = copy.deepcopy(openlineage_event)
    expected_event["outputs"][0]["facets"] = dict()
    expected_event["outputs"][0]["facets"]["version"] = dataset_version_facet

    assert expected_event == event_sent


def test_lifecycle_state_change_dataset_facet():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    lifecycle_state_change_dataset_facet = {
        "lifecycleStateChange": "DROP",
        "previousIdentifier": {
            "namespace": "previous-namespace",
            "name": "previous-name"
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
                "job"
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
                                name="previous-name"
                            )
                        )
                    }
                )
            ]
        )
    )

    event_sent = json.loads(session.post.call_args[0][1])

    dataset_facets = dict()
    dataset_facets["lifecycleStateChange"] = lifecycle_state_change_dataset_facet
    expected_event = copy.deepcopy(openlineage_event)
    expected_event["outputs"][0]["facets"] = dataset_facets

    assert expected_event == event_sent


def test_ownership_dataset_facet():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    ownership_dataset_facet = {
        "owners": [
            {
                "name": "some-owner",
                "type": "some-owner-type"
            }
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
                "job"
            ),
            "some-producer",
            [],
            [
                Dataset(
                    namespace="some-namespace",
                    name="input-dataset",
                    facets={
                        "ownership": OwnershipDatasetFacet(
                            owners=[OwnershipDatasetFacetOwners(
                                name="some-owner",
                                type="some-owner-type"
                            )]
                        )
                    }
                )
            ]
        )
    )

    event_sent = json.loads(session.post.call_args[0][1])

    expected_event = copy.deepcopy(openlineage_event)
    expected_event["outputs"][0]["facets"] = dict()
    expected_event["outputs"][0]["facets"]["ownership"] = ownership_dataset_facet

    assert expected_event == event_sent


def test_column_lineage_dataset_facet():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    column_lineage_dataset_facet = {
        "fields": {
            "output-field": {
                "inputFields": [
                    {
                        "namespace": "namespace-of-input-field-dataset",
                        "name": "name-of-input-field-dataset",
                        "field": "some-field-name"
                    }
                ],
                "transformationDescription": "some-transformation",
                "transformationType": "some-transformation-type"
            }
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
                "job"
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
                                            field="some-field-name"
                                        )
                                    ]
                                )
                            }
                        )
                    }
                )
            ]
        )
    )

    event_sent = json.loads(session.post.call_args[0][1])

    expected_event = copy.deepcopy(openlineage_event)
    expected_event["outputs"][0]["facets"] = dict()
    expected_event["outputs"][0]["facets"]["columnLineage"] = column_lineage_dataset_facet

    assert expected_event == event_sent
