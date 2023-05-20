# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from enum import Enum
from typing import Any

import attr

from openlineage.client.constants import DEFAULT_PRODUCER
from openlineage.client.utils import RedactMixin

SCHEMA_URI = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json"

PRODUCER = DEFAULT_PRODUCER


def set_producer(producer: str) -> None:
    global PRODUCER  # noqa: PLW0603
    PRODUCER = producer


@attr.s
class BaseFacet(RedactMixin):
    _producer: str = attr.ib(init=False)
    _schemaURL: str = attr.ib(init=False)  # noqa: N815

    _base_skip_redact: list[str] = ["_producer", "_schemaURL"]
    _additional_skip_redact: list[str] = []

    def __attrs_post_init__(self) -> None:
        self._producer = PRODUCER
        self._schemaURL = self._get_schema()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/BaseFacet"

    @property
    def skip_redact(self) -> list[str]:
        return self._base_skip_redact + self._additional_skip_redact


@attr.s
class NominalTimeRunFacet(BaseFacet):
    nominalStartTime: str = attr.ib()  # noqa: N815
    nominalEndTime: str | None = attr.ib(default=None)  # noqa: N815

    _additional_skip_redact: list[str] = ["nominalStartTime", "nominalEndTime"]

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/NominalTimeRunFacet"


@attr.s
class ParentRunFacet(BaseFacet):
    run: dict[Any, Any] = attr.ib()
    job: dict[Any, Any] = attr.ib()

    _additional_skip_redact: list[str] = ["job", "run"]

    @classmethod
    def create(cls, runId: str, namespace: str, name: str) -> ParentRunFacet:  # noqa: N803
        return cls(
            run={
                "runId": runId,
            },
            job={
                "namespace": namespace,
                "name": name,
            },
        )

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ParentRunFacet"


@attr.s
class DocumentationJobFacet(BaseFacet):
    description: str = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DocumentationJobFacet"


@attr.s
class SourceCodeLocationJobFacet(BaseFacet):
    type: str = attr.ib()  # noqa: A003
    url: str = attr.ib()

    _additional_skip_redact: list[str] = ["type", "url"]

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SourceCodeLocationJobFacet"


@attr.s
class SqlJobFacet(BaseFacet):
    query: str = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SqlJobFacet"


@attr.s
class DocumentationDatasetFacet(BaseFacet):
    description: str = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DocumentationDatasetFacet"


@attr.s
class SchemaField(RedactMixin):
    name: str = attr.ib()
    type: str = attr.ib()  # noqa: A003
    description: str | None = attr.ib(default=None)

    _do_not_redact = ["name", "type"]


@attr.s
class SchemaDatasetFacet(BaseFacet):
    fields: list[SchemaField] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SchemaDatasetFacet"


@attr.s
class DataSourceDatasetFacet(BaseFacet):
    name: str = attr.ib()
    uri: str = attr.ib()

    _additional_skip_redact: list[str] = ["name", "uri"]

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DataSourceDatasetFacet"


@attr.s
class OutputStatisticsOutputDatasetFacet(BaseFacet):
    rowCount: int = attr.ib()  # noqa: N815
    size: int | None = attr.ib(default=None)

    _additional_skip_redact: list[str] = ["rowCount", "size"]

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/OutputStatisticsOutputDatasetFacet"


@attr.s
class ColumnMetric:
    nullCount: int | None = attr.ib(default=None)  # noqa: N815
    distinctCount: int | None = attr.ib(default=None)  # noqa: N815
    sum: float | None = attr.ib(default=None)  # noqa: A003
    count: int | None = attr.ib(default=None)
    min: float | None = attr.ib(default=None)  # noqa: A003
    max: float | None = attr.ib(default=None)  # noqa: A003
    quantiles: dict[str, float] | None = attr.ib(default=None)


@attr.s
class DataQualityMetricsInputDatasetFacet(BaseFacet):
    rowCount: int | None = attr.ib(default=None)  # noqa: N815
    bytes: int | None = attr.ib(default=None)  # noqa: A003
    columnMetrics: dict[str, ColumnMetric] = attr.ib(factory=dict)  # noqa: N815

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DataQualityMetricsInputDatasetFacet"


@attr.s
class Assertion(RedactMixin):
    assertion: str = attr.ib()
    success: bool = attr.ib()
    column: str | None = attr.ib(default=None)

    _skip_redact: list[str] = ["column"]


@attr.s
class DataQualityAssertionsDatasetFacet(BaseFacet):

    """This facet represents asserted expectations on dataset or it's column."""

    assertions: list[Assertion] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return "#/definitions/DataQualityAssertionsDatasetFacet"


@attr.s
class SourceCodeJobFacet(BaseFacet):

    """This facet represents source code that the job executed."""

    language: str = attr.ib()  # language that the code was written in
    source: str = attr.ib()  # source code text

    _additional_skip_redact: list[str] = ["language"]

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SourceCodeJobFacet"


@attr.s
class ExternalQueryRunFacet(BaseFacet):
    externalQueryId: str = attr.ib()  # noqa: N815
    source: str = attr.ib()


@attr.s
class ErrorMessageRunFacet(BaseFacet):

    """This facet represents an error message that was the result of a job run."""

    message: str = attr.ib()
    programmingLanguage: str = attr.ib()  # noqa: N815
    stackTrace: str | None = attr.ib(default=None)  # noqa: N815

    _additional_skip_redact: list[str] = ["programmingLanguage"]

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ErrorMessageRunFacet"


@attr.s
class SymlinksDatasetFacetIdentifiers:
    namespace: str = attr.ib()
    name: str = attr.ib()
    type: str = attr.ib()  # noqa: A003


@attr.s
class SymlinksDatasetFacet(BaseFacet):

    """This facet represents dataset symlink names."""

    identifiers: list[SymlinksDatasetFacetIdentifiers] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SymlinksDatasetFacet"


@attr.s
class StorageDatasetFacet(BaseFacet):

    """This facet represents dataset symlink names."""

    storageLayer: str = attr.ib()  # noqa: N815
    fileFormat: str = attr.ib()  # noqa: N815

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/StorageDatasetFacet"


@attr.s
class OwnershipJobFacetOwners:
    name: str = attr.ib()
    type: str | None = attr.ib(default=None)  # noqa: A003


@attr.s
class OwnershipJobFacet(BaseFacet):

    """This facet represents ownership of a job."""

    owners: list[OwnershipJobFacetOwners] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/OwnershipJobFacet"


@attr.s
class DatasetVersionDatasetFacet(BaseFacet):

    """This facet represents version of a dataset."""

    datasetVersion: str = attr.ib()  # noqa: N815

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DatasetVersionDatasetFacet"


class LifecycleStateChange(Enum):
    ALTER = "ALTER"
    CREATE = "CREATE"
    DROP = "DROP"
    OVERWRITE = "OVERWRITE"
    RENAME = "RENAME"
    TRUNCATE = "TRUNCATE"


@attr.s
class LifecycleStateChangeDatasetFacetPreviousIdentifier:
    name: str = attr.ib()
    namespace: str = attr.ib()


@attr.s
class LifecycleStateChangeDatasetFacet(BaseFacet):

    """This facet represents information of lifecycle changes of a dataset."""

    lifecycleStateChange: LifecycleStateChange = attr.ib()  # noqa: N815
    previousIdentifier: LifecycleStateChangeDatasetFacetPreviousIdentifier = attr.ib()  # noqa: N815

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/LifecycleStateChangeDatasetFacet"


@attr.s
class OwnershipDatasetFacetOwners:
    name: str = attr.ib()
    type: str = attr.ib()  # noqa: A003


@attr.s
class OwnershipDatasetFacet(BaseFacet):

    """This facet represents ownership of a dataset."""

    owners: list[OwnershipDatasetFacetOwners] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/OwnershipDatasetFacet"


@attr.s
class ColumnLineageDatasetFacetFieldsAdditionalInputFields:
    namespace: str = attr.ib()
    name: str = attr.ib()
    field: str = attr.ib()


@attr.s
class ColumnLineageDatasetFacetFieldsAdditional:
    inputFields: list[  # noqa:  N815
        ColumnLineageDatasetFacetFieldsAdditionalInputFields
    ] = attr.ib()
    transformationDescription: str = attr.ib()  # noqa:  N815
    transformationType: str = attr.ib()  # noqa:  N815


@attr.s
class ColumnLineageDatasetFacet(BaseFacet):

    """This facet contains column lineage of a dataset."""

    fields: dict[str, ColumnLineageDatasetFacetFieldsAdditional] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ColumnLineageDatasetFacet"


@attr.s
class ProcessingEngineRunFacet(BaseFacet):
    version: str = attr.ib()
    name: str = attr.ib()
    openlineageAdapterVersion: str = attr.ib()  # noqa: N815

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ProcessingEngineRunFacet"


@attr.s
class ExtractionError(BaseFacet):
    errorMessage: str = attr.ib()  # noqa: N815
    stackTrace: str | None = attr.ib()  # noqa: N815
    task: str | None = attr.ib()
    taskNumber: int | None = attr.ib()  # noqa: N815


@attr.s
class ExtractionErrorRunFacet(BaseFacet):
    totalTasks: int = attr.ib()  # noqa: N815
    failedTasks: int = attr.ib()  # noqa: N815
    errors: list[ExtractionError] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ExtractionErrorRunFacet"
