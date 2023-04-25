import EventType
from enum import Enum
from datetime import datetime
from typing import List
import LifecycleStateChange
import attrs


@attrs.define
class SchemaDatasetFacetFields:
    name: str
    type: str
    description: str


@attrs.define
class DataQualityAssertionsDatasetFacetAssertions:
    assertion: str
    success: bool
    column: str


@attrs.define
class ParentRunFacetRun:
    runId: str


@attrs.define
class ExtractionErrorRunFacetErrors:
    errorMessage: str
    stackTrace: str
    task: str
    taskNumber: int


@attrs.define
class ParentRunFacetJob:
    namespace: str
    name: str


@attrs.define
class LifecycleStateChangeDatasetFacetPreviousIdentifier:
    name: str
    namespace: str


@attrs.define
class SymlinksDatasetFacetIdentifiers:
    namespace: str
    name: str
    type: str


class EventType(Enum):
    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"


@attrs.define
class ColumnLineageDatasetFacetFieldsAdditionalInputFields:
    namespace: str
    name: str
    field: str


@attrs.define
class BaseFacet:
    _producer: str
    _schemaURL: str


@attrs.define
class DataQualityMetricsInputDatasetFacetColumnMetricsAdditional:
    nullCount: int
    distinctCount: int
    sum: float
    count: float
    min: float
    max: float
    quantiles: dict


class LifecycleStateChange(Enum):
    ALTER = "ALTER"
    CREATE = "CREATE"
    DROP = "DROP"
    OVERWRITE = "OVERWRITE"
    RENAME = "RENAME"
    TRUNCATE = "TRUNCATE"


@attrs.define
class OwnershipJobFacetOwners:
    name: str
    type: str


@attrs.define
class ExtractionErrorRunFacet(BaseFacet):
    totalTasks: int
    failedTasks: int
    errors: List[ExtractionErrorRunFacetErrors]


@attrs.define
class SymlinksDatasetFacet(BaseFacet):
    identifiers: List[SymlinksDatasetFacetIdentifiers]


@attrs.define
class OwnershipDatasetFacetOwners:
    name: str
    type: str


@attrs.define
class DataQualityMetricsInputDatasetFacet(BaseFacet):
    rowCount: int
    bytes: int
    columnMetrics: dict


@attrs.define
class OwnershipJobFacet(BaseFacet):
    owners: List[OwnershipJobFacetOwners]


@attrs.define
class SchemaDatasetFacet(BaseFacet):
    fields: List[SchemaDatasetFacetFields]


@attrs.define
class ColumnLineageDatasetFacetFieldsAdditional:
    inputFields: List[ColumnLineageDatasetFacetFieldsAdditionalInputFields]
    transformationDescription: str
    transformationType: str


@attrs.define
class DatasetVersionDatasetFacet(BaseFacet):
    datasetVersion: str


@attrs.define
class SourceCodeJobFacet(BaseFacet):
    language: str
    sourceCode: str


@attrs.define
class LifecycleStateChangeDatasetFacet(BaseFacet):
    lifecycleStateChange: LifecycleStateChange
    previousIdentifier: LifecycleStateChangeDatasetFacetPreviousIdentifier


@attrs.define
class ColumnLineageDatasetFacet(BaseFacet):
    fields: dict


@attrs.define
class OwnershipDatasetFacet(BaseFacet):
    owners: List[OwnershipDatasetFacetOwners]


@attrs.define
class DataQualityAssertionsDatasetFacet(BaseFacet):
    assertions: List[DataQualityAssertionsDatasetFacetAssertions]


@attrs.define
class SourceCodeLocationJobFacet(BaseFacet):
    type: str
    url: str
    repoUrl: str
    path: str
    version: str
    tag: str
    branch: str


@attrs.define
class ProcessingEngineRunFacet(BaseFacet):
    version: str
    name: str
    openlineageAdapterVersion: str


@attrs.define
class ErrorMessageRunFacet(BaseFacet):
    message: str
    programmingLanguage: str
    stackTrace: str


@attrs.define
class DatasourceDatasetFacet(BaseFacet):
    name: str
    uri: str


@attrs.define
class DocumentationDatasetFacet(BaseFacet):
    description: str


@attrs.define
class ParentRunFacet(BaseFacet):
    run: ParentRunFacetRun
    job: ParentRunFacetJob


@attrs.define
class SQLJobFacet(BaseFacet):
    query: str


@attrs.define
class ExternalQueryRunFacet(BaseFacet):
    externalQueryId: str
    source: str


@attrs.define
class DocumentationJobFacet(BaseFacet):
    description: str


@attrs.define
class NominalTimeRunFacet(BaseFacet):
    nominalStartTime: datetime
    nominalEndTime: datetime


@attrs.define
class StorageDatasetFacet(BaseFacet):
    storageLayer: str
    fileFormat: str


@attrs.define
class OutputStatisticsOutputDatasetFacet(BaseFacet):
    rowCount: int
    size: int


@attrs.define
class JobFacets:
    sourceCode: SourceCodeJobFacet
    ownership: OwnershipJobFacet
    sql: SQLJobFacet
    sourceCodeLocation: SourceCodeLocationJobFacet
    documentation: DocumentationJobFacet


@attrs.define
class RunFacets:
    errorMessage: ErrorMessageRunFacet
    externalQuery: ExternalQueryRunFacet
    extractionError: ExtractionErrorRunFacet
    parent: ParentRunFacet
    nominalTime: NominalTimeRunFacet
    processing_engine: ProcessingEngineRunFacet


@attrs.define
class Run:
    runId: str
    facets: RunFacets


@attrs.define
class InputDatasetInputFacets:
    dataQualityAssertions: DataQualityAssertionsDatasetFacet
    dataQualityMetrics: DataQualityMetricsInputDatasetFacet


@attrs.define
class Job:
    namespace: str
    name: str
    facets: JobFacets


@attrs.define
class DatasetFacets:
    documentation: DocumentationDatasetFacet
    dataSource: DatasourceDatasetFacet
    version: DatasetVersionDatasetFacet
    schema: SchemaDatasetFacet
    ownership: OwnershipDatasetFacet
    storage: StorageDatasetFacet
    columnLineage: ColumnLineageDatasetFacet
    symlinks: SymlinksDatasetFacet
    lifecycleStateChange: LifecycleStateChangeDatasetFacet


@attrs.define
class OutputDatasetOutputFacets:
    outputStatistics: OutputStatisticsOutputDatasetFacet


@attrs.define
class Dataset:
    namespace: str
    name: str
    facets: DatasetFacets


@attrs.define
class OutputDataset(Dataset):
    outputFacets: OutputDatasetOutputFacets


@attrs.define
class InputDataset(Dataset):
    inputFacets: InputDatasetInputFacets


@attrs.define
class RunEvent:
    eventType: EventType
    eventTime: datetime
    run: Run
    job: Job
    inputs: List[InputDataset]
    outputs: List[OutputDataset]
    producer: str
    schemaURL: str


