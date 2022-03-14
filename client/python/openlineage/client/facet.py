# SPDX-License-Identifier: Apache-2.0.

import os
from typing import Dict, List, Optional

import attr

from openlineage.client.constants import DEFAULT_PRODUCER


SCHEMA_URI = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json"

PRODUCER = DEFAULT_PRODUCER


def set_producer(producer):
    global PRODUCER
    PRODUCER = producer


@attr.s
class BaseFacet:
    _producer: str = attr.ib(init=False)
    _schemaURL: str = attr.ib(init=False)

    def __attrs_post_init__(self):
        self._producer = PRODUCER
        self._schemaURL = self._get_schema()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/BaseFacet"


@attr.s
class NominalTimeRunFacet(BaseFacet):
    nominalStartTime: str = attr.ib()
    nominalEndTime: Optional[str] = attr.ib(default=None)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/NominalTimeRunFacet"


@attr.s
class ParentRunFacet(BaseFacet):
    run: Dict = attr.ib()
    job: Dict = attr.ib()

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
class SchemaField:
    name: str = attr.ib()
    type: str = attr.ib()
    description: Optional[str] = attr.ib(default=None)


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

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/DataSourceDatasetFacet"


@attr.s
class OutputStatisticsOutputDatasetFacet(BaseFacet):
    rowCount: int = attr.ib()
    size: Optional[int] = attr.ib(default=None)

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
class Assertion:
    assertion: str = attr.ib()
    success: bool = attr.ib()
    column: Optional[str] = attr.ib(default=None)


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

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/SourceCodeJobFacet"


@attr.s
class ExternalQueryRunFacet(BaseFacet):
    externalQueryId: str = attr.ib()
    source: str = attr.ib()
