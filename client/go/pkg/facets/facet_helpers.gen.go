package facets

import "time"


type InputDatasetFacets struct {
        DataQualityMetricsInputDatasetFacetDataQualityMetrics *DataQualityMetricsInputDatasetFacetDataQualityMetrics `json:"dataQualityMetrics,omitempty"`
        InputStatistics *InputStatistics `json:"inputStatistics,omitempty"`
}

type OutputDatasetFacets struct {
        OutputStatistics *OutputStatistics `json:"outputStatistics,omitempty"`
}

type DatasetFacets struct {
        Subset *Subset `json:"subset,omitempty"`
        Catalog *Catalog `json:"catalog,omitempty"`
        ColumnLineage *ColumnLineage `json:"columnLineage,omitempty"`
        DataQualityAssertions *DataQualityAssertions `json:"dataQualityAssertions,omitempty"`
        DataQualityMetricsDatasetFacetDataQualityMetrics *DataQualityMetricsDatasetFacetDataQualityMetrics `json:"dataQualityMetrics,omitempty"`
        DatasetType *DatasetType `json:"datasetType,omitempty"`
        Version *Version `json:"version,omitempty"`
        DataSource *DataSource `json:"dataSource,omitempty"`
        DocumentationDatasetFacetDocumentation *DocumentationDatasetFacetDocumentation `json:"documentation,omitempty"`
        LifecycleStateChange *LifecycleStateChange `json:"lifecycleStateChange,omitempty"`
        OwnershipDatasetFacetOwnership *OwnershipDatasetFacetOwnership `json:"ownership,omitempty"`
        Schema *Schema `json:"schema,omitempty"`
        Storage *Storage `json:"storage,omitempty"`
        Symlinks *Symlinks `json:"symlinks,omitempty"`
        TagsDatasetFacetTags *TagsDatasetFacetTags `json:"tags,omitempty"`
}

type JobFacets struct {
        DocumentationJobFacetDocumentation *DocumentationJobFacetDocumentation `json:"documentation,omitempty"`
        JobType *JobType `json:"jobType,omitempty"`
        OwnershipJobFacetOwnership *OwnershipJobFacetOwnership `json:"ownership,omitempty"`
        SQL *SQL `json:"sql,omitempty"`
        SourceCode *SourceCode `json:"sourceCode,omitempty"`
        SourceCodeLocation *SourceCodeLocation `json:"sourceCodeLocation,omitempty"`
        TagsJobFacetTags *TagsJobFacetTags `json:"tags,omitempty"`
}

type RunFacets struct {
        EnvironmentVariables *EnvironmentVariables `json:"environmentVariables,omitempty"`
        ErrorMessage *ErrorMessage `json:"errorMessage,omitempty"`
        ExecutionParameters *ExecutionParameters `json:"executionParameters,omitempty"`
        ExternalQuery *ExternalQuery `json:"externalQuery,omitempty"`
        ExtractionError *ExtractionError `json:"extractionError,omitempty"`
        JobDependencies *JobDependencies `json:"jobDependencies,omitempty"`
        NominalTime *NominalTime `json:"nominalTime,omitempty"`
        Parent *Parent `json:"parent,omitempty"`
        ProcessingEngine *ProcessingEngine `json:"processing_engine,omitempty"`
        TagsRunFacetTags *TagsRunFacetTags `json:"tags,omitempty"`
}

var _ InputDatasetFacet = (*DataQualityMetricsInputDatasetFacetDataQualityMetrics)(nil)

func (f *DataQualityMetricsInputDatasetFacetDataQualityMetrics) Apply(facets **InputDatasetFacets) {
        if *facets == nil {
                *facets = &InputDatasetFacets{}
        }
	(*facets).DataQualityMetricsInputDatasetFacetDataQualityMetrics = f
}

var _ InputDatasetFacet = (*InputStatistics)(nil)

func (f *InputStatistics) Apply(facets **InputDatasetFacets) {
        if *facets == nil {
                *facets = &InputDatasetFacets{}
        }
	(*facets).InputStatistics = f
}


var _ OutputDatasetFacet = (*OutputStatistics)(nil)

func (f *OutputStatistics) Apply(facets **OutputDatasetFacets) {
        if *facets == nil {
                *facets = &OutputDatasetFacets{}
        }
	(*facets).OutputStatistics = f
}


var _ DatasetFacet = (*Subset)(nil)

func (f *Subset) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).Subset = f
}

var _ DatasetFacet = (*Catalog)(nil)

func (f *Catalog) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).Catalog = f
}

var _ DatasetFacet = (*ColumnLineage)(nil)

func (f *ColumnLineage) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).ColumnLineage = f
}

var _ DatasetFacet = (*DataQualityAssertions)(nil)

func (f *DataQualityAssertions) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).DataQualityAssertions = f
}

var _ DatasetFacet = (*DataQualityMetricsDatasetFacetDataQualityMetrics)(nil)

func (f *DataQualityMetricsDatasetFacetDataQualityMetrics) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).DataQualityMetricsDatasetFacetDataQualityMetrics = f
}

var _ DatasetFacet = (*DatasetType)(nil)

func (f *DatasetType) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).DatasetType = f
}

var _ DatasetFacet = (*Version)(nil)

func (f *Version) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).Version = f
}

var _ DatasetFacet = (*DataSource)(nil)

func (f *DataSource) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).DataSource = f
}

var _ DatasetFacet = (*DocumentationDatasetFacetDocumentation)(nil)

func (f *DocumentationDatasetFacetDocumentation) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).DocumentationDatasetFacetDocumentation = f
}

var _ DatasetFacet = (*LifecycleStateChange)(nil)

func (f *LifecycleStateChange) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).LifecycleStateChange = f
}

var _ DatasetFacet = (*OwnershipDatasetFacetOwnership)(nil)

func (f *OwnershipDatasetFacetOwnership) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).OwnershipDatasetFacetOwnership = f
}

var _ DatasetFacet = (*Schema)(nil)

func (f *Schema) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).Schema = f
}

var _ DatasetFacet = (*Storage)(nil)

func (f *Storage) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).Storage = f
}

var _ DatasetFacet = (*Symlinks)(nil)

func (f *Symlinks) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).Symlinks = f
}

var _ DatasetFacet = (*TagsDatasetFacetTags)(nil)

func (f *TagsDatasetFacetTags) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).TagsDatasetFacetTags = f
}


var _ JobFacet = (*DocumentationJobFacetDocumentation)(nil)

func (f *DocumentationJobFacetDocumentation) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).DocumentationJobFacetDocumentation = f
}

var _ JobFacet = (*JobType)(nil)

func (f *JobType) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).JobType = f
}

var _ JobFacet = (*OwnershipJobFacetOwnership)(nil)

func (f *OwnershipJobFacetOwnership) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).OwnershipJobFacetOwnership = f
}

var _ JobFacet = (*SQL)(nil)

func (f *SQL) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).SQL = f
}

var _ JobFacet = (*SourceCode)(nil)

func (f *SourceCode) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).SourceCode = f
}

var _ JobFacet = (*SourceCodeLocation)(nil)

func (f *SourceCodeLocation) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).SourceCodeLocation = f
}

var _ JobFacet = (*TagsJobFacetTags)(nil)

func (f *TagsJobFacetTags) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).TagsJobFacetTags = f
}


var _ RunFacet = (*EnvironmentVariables)(nil)

func (f *EnvironmentVariables) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).EnvironmentVariables = f
}

var _ RunFacet = (*ErrorMessage)(nil)

func (f *ErrorMessage) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).ErrorMessage = f
}

var _ RunFacet = (*ExecutionParameters)(nil)

func (f *ExecutionParameters) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).ExecutionParameters = f
}

var _ RunFacet = (*ExternalQuery)(nil)

func (f *ExternalQuery) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).ExternalQuery = f
}

var _ RunFacet = (*ExtractionError)(nil)

func (f *ExtractionError) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).ExtractionError = f
}

var _ RunFacet = (*JobDependencies)(nil)

func (f *JobDependencies) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).JobDependencies = f
}

var _ RunFacet = (*NominalTime)(nil)

func (f *NominalTime) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).NominalTime = f
}

var _ RunFacet = (*Parent)(nil)

func (f *Parent) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).Parent = f
}

var _ RunFacet = (*ProcessingEngine)(nil)

func (f *ProcessingEngine) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).ProcessingEngine = f
}

var _ RunFacet = (*TagsRunFacetTags)(nil)

func (f *TagsRunFacetTags) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).TagsRunFacetTags = f
}




func NewSubset(
        producer string,
) *Subset {
        return &Subset{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json",
        }
}
func (x *Subset) WithInputCondition(inputCondition InputConditionClass) *Subset {
        x.InputCondition = &inputCondition

        return x
}
func (x *Subset) WithOutputCondition(outputCondition InputConditionClass) *Subset {
        x.OutputCondition = &outputCondition

        return x
}


func NewCatalog(
        producer string,
        framework string,
        name string,
        typ string,
) *Catalog {
        return &Catalog{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-1-0/CatalogDatasetFacet.json",
                Framework: framework,
                Name: name,
                Type: typ,
        }
}
func (x *Catalog) WithDeleted(deleted bool) *Catalog {
        x.Deleted = &deleted

        return x
}
func (x *Catalog) WithCatalogProperties(catalogProperties map[string]string) *Catalog {
        x.CatalogProperties = catalogProperties

        return x
}
func (x *Catalog) WithMetadataURI(metadataUri string) *Catalog {
        x.MetadataURI = &metadataUri

        return x
}
func (x *Catalog) WithSource(source string) *Catalog {
        x.Source = &source

        return x
}
func (x *Catalog) WithWarehouseURI(warehouseUri string) *Catalog {
        x.WarehouseURI = &warehouseUri

        return x
}


func NewColumnLineage(
        producer string,
) *ColumnLineage {
        return &ColumnLineage{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
        }
}
func (x *ColumnLineage) WithDeleted(deleted bool) *ColumnLineage {
        x.Deleted = &deleted

        return x
}
func (x *ColumnLineage) WithDataset(dataset []DatasetElement) *ColumnLineage {
        x.Dataset = dataset

        return x
}
func (x *ColumnLineage) WithFields(fields map[string]FieldValue) *ColumnLineage {
        x.Fields = fields

        return x
}


func NewDataQualityAssertions(
        producer string,
) *DataQualityAssertions {
        return &DataQualityAssertions{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-2/DataQualityAssertionsDatasetFacet.json",
        }
}
func (x *DataQualityAssertions) WithAssertions(assertions []Assertion) *DataQualityAssertions {
        x.Assertions = assertions

        return x
}


func NewDataQualityMetricsDatasetFacetDataQualityMetrics(
        producer string,
) *DataQualityMetricsDatasetFacetDataQualityMetrics {
        return &DataQualityMetricsDatasetFacetDataQualityMetrics{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsDatasetFacet.json",
        }
}
func (x *DataQualityMetricsDatasetFacetDataQualityMetrics) WithDeleted(deleted bool) *DataQualityMetricsDatasetFacetDataQualityMetrics {
        x.Deleted = &deleted

        return x
}
func (x *DataQualityMetricsDatasetFacetDataQualityMetrics) WithBytes(bytes int64) *DataQualityMetricsDatasetFacetDataQualityMetrics {
        x.Bytes = &bytes

        return x
}
func (x *DataQualityMetricsDatasetFacetDataQualityMetrics) WithColumnMetrics(columnMetrics map[string]PurpleColumnMetric) *DataQualityMetricsDatasetFacetDataQualityMetrics {
        x.ColumnMetrics = columnMetrics

        return x
}
func (x *DataQualityMetricsDatasetFacetDataQualityMetrics) WithFileCount(fileCount int64) *DataQualityMetricsDatasetFacetDataQualityMetrics {
        x.FileCount = &fileCount

        return x
}
func (x *DataQualityMetricsDatasetFacetDataQualityMetrics) WithLastUpdated(lastUpdated time.Time) *DataQualityMetricsDatasetFacetDataQualityMetrics {
        x.LastUpdated = &lastUpdated

        return x
}
func (x *DataQualityMetricsDatasetFacetDataQualityMetrics) WithRowCount(rowCount int64) *DataQualityMetricsDatasetFacetDataQualityMetrics {
        x.RowCount = &rowCount

        return x
}


func NewDataQualityMetricsInputDatasetFacetDataQualityMetrics(
        producer string,
) *DataQualityMetricsInputDatasetFacetDataQualityMetrics {
        return &DataQualityMetricsInputDatasetFacetDataQualityMetrics{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-3/DataQualityMetricsInputDatasetFacet.json",
        }
}
func (x *DataQualityMetricsInputDatasetFacetDataQualityMetrics) WithBytes(bytes int64) *DataQualityMetricsInputDatasetFacetDataQualityMetrics {
        x.Bytes = &bytes

        return x
}
func (x *DataQualityMetricsInputDatasetFacetDataQualityMetrics) WithColumnMetrics(columnMetrics map[string]FluffyColumnMetric) *DataQualityMetricsInputDatasetFacetDataQualityMetrics {
        x.ColumnMetrics = columnMetrics

        return x
}
func (x *DataQualityMetricsInputDatasetFacetDataQualityMetrics) WithFileCount(fileCount int64) *DataQualityMetricsInputDatasetFacetDataQualityMetrics {
        x.FileCount = &fileCount

        return x
}
func (x *DataQualityMetricsInputDatasetFacetDataQualityMetrics) WithLastUpdated(lastUpdated time.Time) *DataQualityMetricsInputDatasetFacetDataQualityMetrics {
        x.LastUpdated = &lastUpdated

        return x
}
func (x *DataQualityMetricsInputDatasetFacetDataQualityMetrics) WithRowCount(rowCount int64) *DataQualityMetricsInputDatasetFacetDataQualityMetrics {
        x.RowCount = &rowCount

        return x
}


func NewDatasetType(
        producer string,
        datasetType string,
) *DatasetType {
        return &DatasetType{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/DatasetTypeDatasetFacet.json",
                DatasetType: datasetType,
        }
}
func (x *DatasetType) WithDeleted(deleted bool) *DatasetType {
        x.Deleted = &deleted

        return x
}
func (x *DatasetType) WithSubType(subType string) *DatasetType {
        x.SubType = &subType

        return x
}


func NewVersion(
        producer string,
        datasetVersion string,
) *Version {
        return &Version{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json",
                DatasetVersion: datasetVersion,
        }
}
func (x *Version) WithDeleted(deleted bool) *Version {
        x.Deleted = &deleted

        return x
}


func NewDataSource(
        producer string,
) *DataSource {
        return &DataSource{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json",
        }
}
func (x *DataSource) WithDeleted(deleted bool) *DataSource {
        x.Deleted = &deleted

        return x
}
func (x *DataSource) WithName(name string) *DataSource {
        x.Name = &name

        return x
}
func (x *DataSource) WithURI(uri string) *DataSource {
        x.URI = &uri

        return x
}


func NewDocumentationDatasetFacetDocumentation(
        producer string,
        description string,
) *DocumentationDatasetFacetDocumentation {
        return &DocumentationDatasetFacetDocumentation{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-1-0/DocumentationDatasetFacet.json",
                Description: description,
        }
}
func (x *DocumentationDatasetFacetDocumentation) WithDeleted(deleted bool) *DocumentationDatasetFacetDocumentation {
        x.Deleted = &deleted

        return x
}
func (x *DocumentationDatasetFacetDocumentation) WithContentType(contentType string) *DocumentationDatasetFacetDocumentation {
        x.ContentType = &contentType

        return x
}


func NewDocumentationJobFacetDocumentation(
        producer string,
        description string,
) *DocumentationJobFacetDocumentation {
        return &DocumentationJobFacetDocumentation{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json",
                Description: description,
        }
}
func (x *DocumentationJobFacetDocumentation) WithDeleted(deleted bool) *DocumentationJobFacetDocumentation {
        x.Deleted = &deleted

        return x
}
func (x *DocumentationJobFacetDocumentation) WithContentType(contentType string) *DocumentationJobFacetDocumentation {
        x.ContentType = &contentType

        return x
}


func NewEnvironmentVariables(
        producer string,
) *EnvironmentVariables {
        return &EnvironmentVariables{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/EnvironmentVariablesRunFacet.json",
        }
}
func (x *EnvironmentVariables) WithEnvironmentVariables(environmentVariables []EnvironmentVariableElement) *EnvironmentVariables {
        x.EnvironmentVariables = environmentVariables

        return x
}


func NewErrorMessage(
        producer string,
        message string,
        programmingLanguage string,
) *ErrorMessage {
        return &ErrorMessage{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
                Message: message,
                ProgrammingLanguage: programmingLanguage,
        }
}
func (x *ErrorMessage) WithStackTrace(stackTrace string) *ErrorMessage {
        x.StackTrace = &stackTrace

        return x
}


func NewExecutionParameters(
        producer string,
) *ExecutionParameters {
        return &ExecutionParameters{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/ExecutionParametersRunFacet.json",
        }
}
func (x *ExecutionParameters) WithParameters(parameters []ParameterElement) *ExecutionParameters {
        x.Parameters = parameters

        return x
}


func NewExternalQuery(
        producer string,
        externalQueryId string,
        source string,
) *ExternalQuery {
        return &ExternalQuery{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-2/ExternalQueryRunFacet.json",
                ExternalQueryID: externalQueryId,
                Source: source,
        }
}


func NewExtractionError(
        producer string,
        failedTasks int64,
        totalTasks int64,
) *ExtractionError {
        return &ExtractionError{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-1-2/ExtractionErrorRunFacet.json",
                FailedTasks: failedTasks,
                TotalTasks: totalTasks,
        }
}
func (x *ExtractionError) WithErrors(errors []Error) *ExtractionError {
        x.Errors = errors

        return x
}


func NewInputStatistics(
        producer string,
) *InputStatistics {
        return &InputStatistics{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/InputStatisticsInputDatasetFacet.json",
        }
}
func (x *InputStatistics) WithFileCount(fileCount int64) *InputStatistics {
        x.FileCount = &fileCount

        return x
}
func (x *InputStatistics) WithRowCount(rowCount int64) *InputStatistics {
        x.RowCount = &rowCount

        return x
}
func (x *InputStatistics) WithSize(size int64) *InputStatistics {
        x.Size = &size

        return x
}


func NewJobDependencies(
        producer string,
) *JobDependencies {
        return &JobDependencies{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/JobDependenciesRunFacet.json",
        }
}
func (x *JobDependencies) WithDownstream(downstream []DownstreamElement) *JobDependencies {
        x.Downstream = downstream

        return x
}
func (x *JobDependencies) WithTriggerRule(triggerRule string) *JobDependencies {
        x.TriggerRule = &triggerRule

        return x
}
func (x *JobDependencies) WithUpstream(upstream []DownstreamElement) *JobDependencies {
        x.Upstream = upstream

        return x
}


func NewJobType(
        producer string,
        integration string,
        processingType string,
) *JobType {
        return &JobType{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json",
                Integration: integration,
                ProcessingType: processingType,
        }
}
func (x *JobType) WithDeleted(deleted bool) *JobType {
        x.Deleted = &deleted

        return x
}
func (x *JobType) WithJobType(jobType string) *JobType {
        x.JobType = &jobType

        return x
}


func NewLifecycleStateChange(
        producer string,
        lifecycleStateChange LifecycleStateChangeEnum,
) *LifecycleStateChange {
        return &LifecycleStateChange{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/LifecycleStateChangeDatasetFacet.json",
                LifecycleStateChange: lifecycleStateChange,
        }
}
func (x *LifecycleStateChange) WithDeleted(deleted bool) *LifecycleStateChange {
        x.Deleted = &deleted

        return x
}
func (x *LifecycleStateChange) WithPreviousIdentifier(previousIdentifier PreviousIdentifier) *LifecycleStateChange {
        x.PreviousIdentifier = &previousIdentifier

        return x
}


func NewNominalTime(
        producer string,
        nominalStartTime time.Time,
) *NominalTime {
        return &NominalTime{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json",
                NominalStartTime: nominalStartTime,
        }
}
func (x *NominalTime) WithNominalEndTime(nominalEndTime time.Time) *NominalTime {
        x.NominalEndTime = &nominalEndTime

        return x
}


func NewOutputStatistics(
        producer string,
) *OutputStatistics {
        return &OutputStatistics{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json",
        }
}
func (x *OutputStatistics) WithFileCount(fileCount int64) *OutputStatistics {
        x.FileCount = &fileCount

        return x
}
func (x *OutputStatistics) WithRowCount(rowCount int64) *OutputStatistics {
        x.RowCount = &rowCount

        return x
}
func (x *OutputStatistics) WithSize(size int64) *OutputStatistics {
        x.Size = &size

        return x
}


func NewOwnershipDatasetFacetOwnership(
        producer string,
) *OwnershipDatasetFacetOwnership {
        return &OwnershipDatasetFacetOwnership{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json",
        }
}
func (x *OwnershipDatasetFacetOwnership) WithDeleted(deleted bool) *OwnershipDatasetFacetOwnership {
        x.Deleted = &deleted

        return x
}
func (x *OwnershipDatasetFacetOwnership) WithOwners(owners []PurpleOwner) *OwnershipDatasetFacetOwnership {
        x.Owners = owners

        return x
}


func NewOwnershipJobFacetOwnership(
        producer string,
) *OwnershipJobFacetOwnership {
        return &OwnershipJobFacetOwnership{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json",
        }
}
func (x *OwnershipJobFacetOwnership) WithDeleted(deleted bool) *OwnershipJobFacetOwnership {
        x.Deleted = &deleted

        return x
}
func (x *OwnershipJobFacetOwnership) WithOwners(owners []FluffyOwner) *OwnershipJobFacetOwnership {
        x.Owners = owners

        return x
}


func NewParent(
        producer string,
        job ParentJob,
        run ParentRun,
) *Parent {
        return &Parent{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json",
                Job: job,
                Run: run,
        }
}
func (x *Parent) WithRoot(root Root) *Parent {
        x.Root = &root

        return x
}


func NewProcessingEngine(
        producer string,
        version string,
) *ProcessingEngine {
        return &ProcessingEngine{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json",
                Version: version,
        }
}
func (x *ProcessingEngine) WithName(name string) *ProcessingEngine {
        x.Name = &name

        return x
}
func (x *ProcessingEngine) WithOpenlineageAdapterVersion(openlineageAdapterVersion string) *ProcessingEngine {
        x.OpenlineageAdapterVersion = &openlineageAdapterVersion

        return x
}


func NewSQL(
        producer string,
        query string,
) *SQL {
        return &SQL{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json",
                Query: query,
        }
}
func (x *SQL) WithDeleted(deleted bool) *SQL {
        x.Deleted = &deleted

        return x
}
func (x *SQL) WithDialect(dialect string) *SQL {
        x.Dialect = &dialect

        return x
}


func NewSchema(
        producer string,
) *Schema {
        return &Schema{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json",
        }
}
func (x *Schema) WithDeleted(deleted bool) *Schema {
        x.Deleted = &deleted

        return x
}
func (x *Schema) WithFields(fields []FieldElement) *Schema {
        x.Fields = fields

        return x
}


func NewSourceCode(
        producer string,
        language string,
        sourceCode string,
) *SourceCode {
        return &SourceCode{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/SourceCodeJobFacet.json",
                Language: language,
                SourceCode: sourceCode,
        }
}
func (x *SourceCode) WithDeleted(deleted bool) *SourceCode {
        x.Deleted = &deleted

        return x
}


func NewSourceCodeLocation(
        producer string,
        typ string,
        url string,
) *SourceCodeLocation {
        return &SourceCodeLocation{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/SourceCodeLocationJobFacet.json",
                Type: typ,
                URL: url,
        }
}
func (x *SourceCodeLocation) WithDeleted(deleted bool) *SourceCodeLocation {
        x.Deleted = &deleted

        return x
}
func (x *SourceCodeLocation) WithBranch(branch string) *SourceCodeLocation {
        x.Branch = &branch

        return x
}
func (x *SourceCodeLocation) WithPath(path string) *SourceCodeLocation {
        x.Path = &path

        return x
}
func (x *SourceCodeLocation) WithRepoURL(repoUrl string) *SourceCodeLocation {
        x.RepoURL = &repoUrl

        return x
}
func (x *SourceCodeLocation) WithTag(tag string) *SourceCodeLocation {
        x.Tag = &tag

        return x
}
func (x *SourceCodeLocation) WithVersion(version string) *SourceCodeLocation {
        x.Version = &version

        return x
}


func NewStorage(
        producer string,
        storageLayer string,
) *Storage {
        return &Storage{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/StorageDatasetFacet.json",
                StorageLayer: storageLayer,
        }
}
func (x *Storage) WithDeleted(deleted bool) *Storage {
        x.Deleted = &deleted

        return x
}
func (x *Storage) WithFileFormat(fileFormat string) *Storage {
        x.FileFormat = &fileFormat

        return x
}


func NewSymlinks(
        producer string,
) *Symlinks {
        return &Symlinks{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json",
        }
}
func (x *Symlinks) WithDeleted(deleted bool) *Symlinks {
        x.Deleted = &deleted

        return x
}
func (x *Symlinks) WithIdentifiers(identifiers []Identifier) *Symlinks {
        x.Identifiers = identifiers

        return x
}


func NewTagsDatasetFacetTags(
        producer string,
) *TagsDatasetFacetTags {
        return &TagsDatasetFacetTags{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json",
        }
}
func (x *TagsDatasetFacetTags) WithDeleted(deleted bool) *TagsDatasetFacetTags {
        x.Deleted = &deleted

        return x
}
func (x *TagsDatasetFacetTags) WithTags(tags []TagElement) *TagsDatasetFacetTags {
        x.Tags = tags

        return x
}


func NewTagsJobFacetTags(
        producer string,
) *TagsJobFacetTags {
        return &TagsJobFacetTags{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/TagsJobFacet.json",
        }
}
func (x *TagsJobFacetTags) WithDeleted(deleted bool) *TagsJobFacetTags {
        x.Deleted = &deleted

        return x
}
func (x *TagsJobFacetTags) WithTags(tags []TagClass) *TagsJobFacetTags {
        x.Tags = tags

        return x
}


func NewTagsRunFacetTags(
        producer string,
) *TagsRunFacetTags {
        return &TagsRunFacetTags{
                Producer: producer,
                SchemaURL: "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json",
        }
}
func (x *TagsRunFacetTags) WithTags(tags []TagsTag) *TagsRunFacetTags {
        x.Tags = tags

        return x
}



