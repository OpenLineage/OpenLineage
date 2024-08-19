package facets


type InputDatasetFacets struct {
        DataQualityMetrics *DataQualityMetrics `json:"dataQualityMetrics,omitempty"`
}

type OutputDatasetFacets struct {
        OutputStatistics *OutputStatistics `json:"outputStatistics,omitempty"`
}

type DatasetFacets struct {
        ColumnLineage *ColumnLineage `json:"columnLineage,omitempty"`
        DataQualityAssertions *DataQualityAssertions `json:"dataQualityAssertions,omitempty"`
        Version *Version `json:"version,omitempty"`
        DataSource *DataSource `json:"dataSource,omitempty"`
        DatasetDocumentation *DatasetDocumentation `json:"documentation,omitempty"`
        LifecycleStateChange *LifecycleStateChange `json:"lifecycleStateChange,omitempty"`
        DatasetOwnership *DatasetOwnership `json:"ownership,omitempty"`
        Schema *Schema `json:"schema,omitempty"`
        Storage *Storage `json:"storage,omitempty"`
        Symlinks *Symlinks `json:"symlinks,omitempty"`
}

type JobFacets struct {
        JobDocumentation *JobDocumentation `json:"documentation,omitempty"`
        JobType *JobType `json:"jobType,omitempty"`
        JobOwnership *JobOwnership `json:"ownership,omitempty"`
        SourceCode *SourceCode `json:"sourceCode,omitempty"`
        SourceCodeLocation *SourceCodeLocation `json:"sourceCodeLocation,omitempty"`
        SQL *SQL `json:"sql,omitempty"`
}

type RunFacets struct {
        ErrorMessage *ErrorMessage `json:"errorMessage,omitempty"`
        ExternalQuery *ExternalQuery `json:"externalQuery,omitempty"`
        ExtractionError *ExtractionError `json:"extractionError,omitempty"`
        NominalTime *NominalTime `json:"nominalTime,omitempty"`
        Parent *Parent `json:"parent,omitempty"`
        ProcessingEngine *ProcessingEngine `json:"processing_engine,omitempty"`
}

var _ InputDatasetFacet = (*DataQualityMetrics)(nil)

func (f *DataQualityMetrics) Apply(facets **InputDatasetFacets) {
        if *facets == nil {
                *facets = &InputDatasetFacets{}
        }
	(*facets).DataQualityMetrics = f
}


var _ OutputDatasetFacet = (*OutputStatistics)(nil)

func (f *OutputStatistics) Apply(facets **OutputDatasetFacets) {
        if *facets == nil {
                *facets = &OutputDatasetFacets{}
        }
	(*facets).OutputStatistics = f
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

var _ DatasetFacet = (*DatasetDocumentation)(nil)

func (f *DatasetDocumentation) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).DatasetDocumentation = f
}

var _ DatasetFacet = (*LifecycleStateChange)(nil)

func (f *LifecycleStateChange) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).LifecycleStateChange = f
}

var _ DatasetFacet = (*DatasetOwnership)(nil)

func (f *DatasetOwnership) Apply(facets **DatasetFacets) {
        if *facets == nil {
                *facets = &DatasetFacets{}
        }
	(*facets).DatasetOwnership = f
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


var _ JobFacet = (*JobDocumentation)(nil)

func (f *JobDocumentation) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).JobDocumentation = f
}

var _ JobFacet = (*JobType)(nil)

func (f *JobType) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).JobType = f
}

var _ JobFacet = (*JobOwnership)(nil)

func (f *JobOwnership) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).JobOwnership = f
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

var _ JobFacet = (*SQL)(nil)

func (f *SQL) Apply(facets **JobFacets) {
        if *facets == nil {
                *facets = &JobFacets{}
        }
	(*facets).SQL = f
}


var _ RunFacet = (*ErrorMessage)(nil)

func (f *ErrorMessage) Apply(facets **RunFacets) {
        if *facets == nil {
                *facets = &RunFacets{}
        }
	(*facets).ErrorMessage = f
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




func NewColumnLineage(
) *ColumnLineage {
        return &ColumnLineage{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-1-0/ColumnLineageDatasetFacet.json",
        }
}
func (x *ColumnLineage) WithDeleted(deleted bool) *ColumnLineage {
        x.Deleted = &deleted

        return x
}
func (x *ColumnLineage) WithFields(fields map[string]Field) *ColumnLineage {
        x.Fields = fields

        return x
}


func NewDataQualityAssertions(
) *DataQualityAssertions {
        return &DataQualityAssertions{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
        }
}
func (x *DataQualityAssertions) WithAssertions(assertions []Assertion) *DataQualityAssertions {
        x.Assertions = assertions

        return x
}


func NewDataQualityMetrics(
) *DataQualityMetrics {
        return &DataQualityMetrics{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsInputDatasetFacet.json",
        }
}
func (x *DataQualityMetrics) WithBytes(bytes int64) *DataQualityMetrics {
        x.Bytes = &bytes

        return x
}
func (x *DataQualityMetrics) WithColumnMetrics(columnMetrics map[string]ColumnMetric) *DataQualityMetrics {
        x.ColumnMetrics = columnMetrics

        return x
}
func (x *DataQualityMetrics) WithFileCount(fileCount int64) *DataQualityMetrics {
        x.FileCount = &fileCount

        return x
}
func (x *DataQualityMetrics) WithRowCount(rowCount int64) *DataQualityMetrics {
        x.RowCount = &rowCount

        return x
}


func NewVersion(
        datasetVersion string,
) *Version {
        return &Version{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json",
                DatasetVersion: datasetVersion,
        }
}
func (x *Version) WithDeleted(deleted bool) *Version {
        x.Deleted = &deleted

        return x
}


func NewDataSource(
) *DataSource {
        return &DataSource{
                Producer: "openlineage-go",
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


func NewDatasetDocumentation(
        description string,
) *DatasetDocumentation {
        return &DatasetDocumentation{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json",
                Description: description,
        }
}
func (x *DatasetDocumentation) WithDeleted(deleted bool) *DatasetDocumentation {
        x.Deleted = &deleted

        return x
}


func NewJobDocumentation(
        description string,
) *JobDocumentation {
        return &JobDocumentation{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/DocumentationJobFacet.json",
                Description: description,
        }
}
func (x *JobDocumentation) WithDeleted(deleted bool) *JobDocumentation {
        x.Deleted = &deleted

        return x
}


func NewErrorMessage(
        message string,
        programmingLanguage string,
) *ErrorMessage {
        return &ErrorMessage{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
                Message: message,
                ProgrammingLanguage: programmingLanguage,
        }
}
func (x *ErrorMessage) WithStackTrace(stackTrace string) *ErrorMessage {
        x.StackTrace = &stackTrace

        return x
}


func NewExternalQuery(
        externalQueryId string,
        source string,
) *ExternalQuery {
        return &ExternalQuery{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-2/ExternalQueryRunFacet.json",
                ExternalQueryID: externalQueryId,
                Source: source,
        }
}


func NewExtractionError(
        failedTasks int64,
        totalTasks int64,
) *ExtractionError {
        return &ExtractionError{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-1-2/ExtractionErrorRunFacet.json",
                FailedTasks: failedTasks,
                TotalTasks: totalTasks,
        }
}
func (x *ExtractionError) WithErrors(errors []Error) *ExtractionError {
        x.Errors = errors

        return x
}


func NewJobType(
        integration string,
        processingType string,
) *JobType {
        return &JobType{
                Producer: "openlineage-go",
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
        lifecycleStateChange LifecycleStateChangeEnum,
) *LifecycleStateChange {
        return &LifecycleStateChange{
                Producer: "openlineage-go",
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
        nominalStartTime string,
) *NominalTime {
        return &NominalTime{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json",
                NominalStartTime: nominalStartTime,
        }
}
func (x *NominalTime) WithNominalEndTime(nominalEndTime string) *NominalTime {
        x.NominalEndTime = &nominalEndTime

        return x
}


func NewOutputStatistics(
) *OutputStatistics {
        return &OutputStatistics{
                Producer: "openlineage-go",
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


func NewDatasetOwnership(
) *DatasetOwnership {
        return &DatasetOwnership{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json",
        }
}
func (x *DatasetOwnership) WithDeleted(deleted bool) *DatasetOwnership {
        x.Deleted = &deleted

        return x
}
func (x *DatasetOwnership) WithOwners(owners []DatasetOwner) *DatasetOwnership {
        x.Owners = owners

        return x
}


func NewJobOwnership(
) *JobOwnership {
        return &JobOwnership{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json",
        }
}
func (x *JobOwnership) WithDeleted(deleted bool) *JobOwnership {
        x.Deleted = &deleted

        return x
}
func (x *JobOwnership) WithOwners(owners []JobOwner) *JobOwnership {
        x.Owners = owners

        return x
}


func NewParent(
        job Job,
        run Run,
) *Parent {
        return &Parent{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json",
                Job: job,
                Run: run,
        }
}


func NewProcessingEngine(
        version string,
) *ProcessingEngine {
        return &ProcessingEngine{
                Producer: "openlineage-go",
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


func NewSchema(
) *Schema {
        return &Schema{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
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
        language string,
        sourceCode string,
) *SourceCode {
        return &SourceCode{
                Producer: "openlineage-go",
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
        typ string,
        url string,
) *SourceCodeLocation {
        return &SourceCodeLocation{
                Producer: "openlineage-go",
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


func NewSQL(
        query string,
) *SQL {
        return &SQL{
                Producer: "openlineage-go",
                SchemaURL: "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json",
                Query: query,
        }
}
func (x *SQL) WithDeleted(deleted bool) *SQL {
        x.Deleted = &deleted

        return x
}


func NewStorage(
        storageLayer string,
) *Storage {
        return &Storage{
                Producer: "openlineage-go",
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
) *Symlinks {
        return &Symlinks{
                Producer: "openlineage-go",
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



