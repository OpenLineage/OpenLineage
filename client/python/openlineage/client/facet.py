# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Dict, List, Optional

import attr
from openlineage.client.constants import DEFAULT_PRODUCER
from openlineage.client.utils import RedactMixin

SCHEMA_URI = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json"

PRODUCER = DEFAULT_PRODUCER


def set_producer(producer):
    global PRODUCER
    PRODUCER = producer


@attr.s
class BaseFacet(RedactMixin):
    _producer: str = attr.ib(init=False)
    _schemaURL: str = attr.ib(init=False)

    _base_skip_redact: List[str] = ['_producer', '_schemaURL']
    _additional_skip_redact: List[str] = []

    def __attrs_post_init__(self):
        self._producer = PRODUCER
        self._schemaURL = self._get_schema()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/BaseFacet"

    @property
    def skip_redact(self):
        return self._base_skip_redact + self._additional_skip_redact


@attr.s
class NominalTimeRunFacet(BaseFacet):
    nominalStartTime: str = attr.ib()
    nominalEndTime: Optional[str] = attr.ib(default=None)

    _additional_skip_redact: List[str] = ['nominalStartTime', 'nominalEndTime']

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/NominalTimeRunFacet"


@attr.s
class ParentRunFacet(BaseFacet):
    run: Dict = attr.ib()
    job: Dict = attr.ib()

    _additional_skip_redact: List[str] = ['job', 'run']

    @classmethod
    def create(cls, runId: str, namespace: str, name: str):
        return cls(
            run={
                "runId": runId
            },
            job={
                "namespace": namespace,
                "name": name
            }
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
    type: str = attr.ib()
    url: str = attr.ib()

    _additional_skip_redact: List[str] = ['type', 'url']

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
    type: str = attr.ib()
    description: Optional[str] = attr.ib(default=None)

    _do_not_redact = ['name', 'type']


@attr.s
class SchemaDatasetFacet(BaseFacet):
    fields: List[SchemaField] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SchemaDatasetFacet"


@attr.s
class DataSourceDatasetFacet(BaseFacet):
    name: str = attr.ib()
    uri: str = attr.ib()

    _additional_skip_redact: List[str] = ['name', 'uri']

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DataSourceDatasetFacet"


@attr.s
class OutputStatisticsOutputDatasetFacet(BaseFacet):
    rowCount: int = attr.ib()
    size: Optional[int] = attr.ib(default=None)

    _additional_skip_redact: List[str] = ['rowCount', 'size']

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/OutputStatisticsOutputDatasetFacet"


@attr.s
class ColumnMetric:
    nullCount: Optional[int] = attr.ib(default=None)
    distinctCount: Optional[int] = attr.ib(default=None)
    sum: Optional[float] = attr.ib(default=None)
    count: Optional[int] = attr.ib(default=None)
    min: Optional[float] = attr.ib(default=None)
    max: Optional[float] = attr.ib(default=None)
    quantiles: Optional[Dict[str, float]] = attr.ib(default=None)


@attr.s
class DataQualityMetricsInputDatasetFacet(BaseFacet):
    rowCount: Optional[int] = attr.ib(default=None)
    bytes: Optional[int] = attr.ib(default=None)
    columnMetrics: Dict[str, ColumnMetric] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DataQualityMetricsInputDatasetFacet"


@attr.s
class Assertion(RedactMixin):
    assertion: str = attr.ib()
    success: bool = attr.ib()
    column: Optional[str] = attr.ib(default=None)

    _skip_redact: List[str] = ['column']


@attr.s
class DataQualityAssertionsDatasetFacet(BaseFacet):
    """
    This facet represents asserted expectations on dataset or it's column
    """
    assertions: List[Assertion] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return "#/definitions/DataQualityAssertionsDatasetFacet"  # noqa


@attr.s
class SourceCodeJobFacet(BaseFacet):
    """
    This facet represents source code that the job executed.
    """
    language: str = attr.ib()  # language that the code was written in
    source: str = attr.ib()  # source code text

    _additional_skip_redact: List[str] = ['language']

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SourceCodeJobFacet"


@attr.s
class ExternalQueryRunFacet(BaseFacet):
    externalQueryId: str = attr.ib()
    source: str = attr.ib()


@attr.s
class ErrorMessageRunFacet(BaseFacet):
    """
    This facet represents an error message that was the result of a job run
    """
    message: str = attr.ib()
    programmingLanguage: str = attr.ib()
    stackTrace: Optional[str] = attr.ib(default=None)

    _additional_skip_redact: List[str] = ['programmingLanguage']

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ErrorMessageRunFacet"


@attr.s
class SymlinksDatasetFacetIdentifiers:
    namespace: str = attr.ib()
    name: str = attr.ib()
    type: str = attr.ib()


@attr.s
class SymlinksDatasetFacet(BaseFacet):
    """
    This facet represents dataset symlink names
    """
    identifiers: List[SymlinksDatasetFacetIdentifiers] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SymlinksDatasetFacet"


@attr.s
class StorageDatasetFacet(BaseFacet):
    """
    This facet represents dataset symlink names
    """
    storageLayer: str = attr.ib()
    fileFormat: str = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/StorageDatasetFacet"


@attr.s
class OwnershipJobFacetOwners:
    name: str = attr.ib()
    type: Optional[str] = attr.ib(default=None)


@attr.s
class OwnershipJobFacet(BaseFacet):
    """
    This facet represents ownership of a job
    """
    owners: List[OwnershipJobFacetOwners] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/OwnershipJobFacet"


@attr.s
class DatasetVersionDatasetFacet(BaseFacet):
    """
    This facet represents version of a dataset
    """
    datasetVersion: str = attr.ib()

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
class LifecycleStateChangeDatasetFacetPreviousIdentifier():
    name: str = attr.ib()
    namespace: str = attr.ib()


@attr.s
class LifecycleStateChangeDatasetFacet(BaseFacet):
    """
    This facet represents information of lifecycle changes of a dataset
    """
    lifecycleStateChange: LifecycleStateChange = attr.ib()
    previousIdentifier: LifecycleStateChangeDatasetFacetPreviousIdentifier = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/LifecycleStateChangeDatasetFacet"


@attr.s
class OwnershipDatasetFacetOwners:
    name: str = attr.ib()
    type: str = attr.ib()


@attr.s
class OwnershipDatasetFacet(BaseFacet):
    """
    This facet represents ownership of a dataset
    """
    owners: List[OwnershipDatasetFacetOwners] = attr.ib(factory=dict)

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
    inputFields: List[ColumnLineageDatasetFacetFieldsAdditionalInputFields] = attr.ib()
    transformationDescription: str = attr.ib()
    transformationType: str = attr.ib()


@attr.s
class ColumnLineageDatasetFacet(BaseFacet):
    """
    This facet contains column lineage of a dataset
    """
    fields: Dict[str, ColumnLineageDatasetFacetFieldsAdditional] = attr.ib(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ColumnLineageDatasetFacet"


@attr.s
class ProcessingEngineRunFacet(BaseFacet):
    version: str = attr.ib()
    name: str = attr.ib()
    openlineageAdapterVersion: str = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ProcessingEngineRunFacet"


@attr.s
class ExtractionError(BaseFacet):
    errorMessage: str = attr.ib()
    stackTrace: Optional[str] = attr.ib()
    task: Optional[str] = attr.ib()
    taskNumber: Optional[int] = attr.ib()


@attr.s
class ExtractionErrorRunFacet(BaseFacet):
    totalTasks: int = attr.ib()
    failedTasks: int = attr.ib()
    errors: List[ExtractionError] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ExtractionErrorRunFacet"
