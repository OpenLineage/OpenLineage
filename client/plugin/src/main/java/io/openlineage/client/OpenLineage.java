package io.openlineage.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class OpenLineage {
  interface BaseFacet {
    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    String get_producer();

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    String get_schemaURL();
  }

  static final class SourceCodeLocationJobFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final String type;

    private final String url;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param type
     * @param url
     */
    @JsonCreator
    SourceCodeLocationJobFacet(@JsonProperty("_producer") String _producer,
        @JsonProperty("type") String type, @JsonProperty("url") String url) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet";
      this.type = type;
      this.url = url;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    public String getType() {
      return type;
    }

    public String getUrl() {
      return url;
    }
  }

  static final class ParentRunFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final ParentRunFacetRun run;

    private final ParentRunFacetJob job;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param run
     * @param job
     */
    @JsonCreator
    ParentRunFacet(@JsonProperty("_producer") String _producer,
        @JsonProperty("run") ParentRunFacetRun run, @JsonProperty("job") ParentRunFacetJob job) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/ParentRunFacet";
      this.run = run;
      this.job = job;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    public ParentRunFacetRun getRun() {
      return run;
    }

    public ParentRunFacetJob getJob() {
      return job;
    }
  }

  static final class DatasetFacets {
    private final DocumentationDatasetFacet documentation;

    private final SchemaDatasetFacet schema;

    private final DatasourceDatasetFacet dataSource;

    @JsonAnySetter
    public final Map<String, CustomFacet> additionalProperties;

    /**
     * @param documentation
     * @param schema
     * @param dataSource
     */
    @JsonCreator
    DatasetFacets(@JsonProperty("documentation") DocumentationDatasetFacet documentation,
        @JsonProperty("schema") SchemaDatasetFacet schema,
        @JsonProperty("dataSource") DatasourceDatasetFacet dataSource) {
      this.documentation = documentation;
      this.schema = schema;
      this.dataSource = dataSource;
      this.additionalProperties = new HashMap<>();
    }

    public DocumentationDatasetFacet getDocumentation() {
      return documentation;
    }

    public SchemaDatasetFacet getSchema() {
      return schema;
    }

    public DatasourceDatasetFacet getDataSource() {
      return dataSource;
    }

    /**
     * additional properties
     */
    @JsonAnyGetter
    public Map<String, CustomFacet> getAdditionalProperties() {
      return additionalProperties;
    }
  }

  static final class Dataset {
    private final String namespace;

    private final String name;

    private final DatasetFacets facets;

    /**
     * @param namespace The namespace containing that dataset
     * @param name The unique name for that dataset within that namespace
     * @param facets The facets for this dataset
     */
    @JsonCreator
    Dataset(@JsonProperty("namespace") String namespace, @JsonProperty("name") String name,
        @JsonProperty("facets") DatasetFacets facets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
    }

    /**
     * The namespace containing that dataset
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * The unique name for that dataset within that namespace
     */
    public String getName() {
      return name;
    }

    /**
     * The facets for this dataset
     */
    public DatasetFacets getFacets() {
      return facets;
    }
  }

  static final class NominalTimeRunFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final String nominalStartTime;

    private final String nominalEndTime;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param nominalStartTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time
     * @param nominalEndTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run)
     */
    @JsonCreator
    NominalTimeRunFacet(@JsonProperty("_producer") String _producer,
        @JsonProperty("nominalStartTime") String nominalStartTime,
        @JsonProperty("nominalEndTime") String nominalEndTime) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/NominalTimeRunFacet";
      this.nominalStartTime = nominalStartTime;
      this.nominalEndTime = nominalEndTime;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    /**
     * An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time
     */
    public String getNominalStartTime() {
      return nominalStartTime;
    }

    /**
     * An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run)
     */
    public String getNominalEndTime() {
      return nominalEndTime;
    }
  }

  static final class SchemaDatasetFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final List<SchemaDatasetFacetFields> fields;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param fields The fields of the table.
     */
    @JsonCreator
    SchemaDatasetFacet(@JsonProperty("_producer") String _producer,
        @JsonProperty("fields") List<SchemaDatasetFacetFields> fields) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/SchemaDatasetFacet";
      this.fields = fields;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    /**
     * The fields of the table.
     */
    public List<SchemaDatasetFacetFields> getFields() {
      return fields;
    }
  }

  static final class SQLJobFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final String query;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param query
     */
    @JsonCreator
    SQLJobFacet(@JsonProperty("_producer") String _producer, @JsonProperty("query") String query) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/SQLJobFacet";
      this.query = query;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    public String getQuery() {
      return query;
    }
  }

  static final class Run {
    private final String runId;

    private final RunFacets facets;

    /**
     * @param runId The id of the run, unique relative to the job
     * @param facets The run facets.
     */
    @JsonCreator
    Run(@JsonProperty("runId") String runId, @JsonProperty("facets") RunFacets facets) {
      this.runId = runId;
      this.facets = facets;
    }

    /**
     * The id of the run, unique relative to the job
     */
    public String getRunId() {
      return runId;
    }

    /**
     * The run facets.
     */
    public RunFacets getFacets() {
      return facets;
    }
  }

  static final class CustomFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    @JsonAnySetter
    public final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @JsonCreator
    CustomFacet(@JsonProperty("_producer") String _producer) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/CustomFacet";
      this.additionalProperties = new HashMap<>();
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    /**
     * additional properties
     */
    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }
  }

  static final class ParentRunFacetRun {
    private final String runId;

    /**
     * @param runId
     */
    @JsonCreator
    ParentRunFacetRun(@JsonProperty("runId") String runId) {
      this.runId = runId;
    }

    public String getRunId() {
      return runId;
    }
  }

  static final class DatasourceDatasetFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final String name;

    private final String uri;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param name
     * @param uri
     */
    @JsonCreator
    DatasourceDatasetFacet(@JsonProperty("_producer") String _producer,
        @JsonProperty("name") String name, @JsonProperty("uri") String uri) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/DatasourceDatasetFacet";
      this.name = name;
      this.uri = uri;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    public String getName() {
      return name;
    }

    public String getUri() {
      return uri;
    }
  }

  static final class RunStateUpdate {
    private final String eventType;

    private final String eventTime;

    private final Run run;

    private final Job job;

    private final List<Dataset> inputs;

    private final List<Dataset> outputs;

    private final String producer;

    /**
     * @param eventType the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete
     * @param eventTime the time the event occured at
     * @param run
     * @param job
     * @param inputs The set of **input** datasets.
     * @param outputs The set of **output** datasets.
     * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @JsonCreator
    RunStateUpdate(@JsonProperty("eventType") String eventType,
        @JsonProperty("eventTime") String eventTime, @JsonProperty("run") Run run,
        @JsonProperty("job") Job job, @JsonProperty("inputs") List<Dataset> inputs,
        @JsonProperty("outputs") List<Dataset> outputs, @JsonProperty("producer") String producer) {
      this.eventType = eventType;
      this.eventTime = eventTime;
      this.run = run;
      this.job = job;
      this.inputs = inputs;
      this.outputs = outputs;
      this.producer = producer;
    }

    /**
     * the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete
     */
    public String getEventType() {
      return eventType;
    }

    /**
     * the time the event occured at
     */
    public String getEventTime() {
      return eventTime;
    }

    public Run getRun() {
      return run;
    }

    public Job getJob() {
      return job;
    }

    /**
     * The set of **input** datasets.
     */
    public List<Dataset> getInputs() {
      return inputs;
    }

    /**
     * The set of **output** datasets.
     */
    public List<Dataset> getOutputs() {
      return outputs;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String getProducer() {
      return producer;
    }
  }

  static final class DocumentationJobFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final String description;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param description The description of the job.
     */
    @JsonCreator
    DocumentationJobFacet(@JsonProperty("_producer") String _producer,
        @JsonProperty("description") String description) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/DocumentationJobFacet";
      this.description = description;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    /**
     * The description of the job.
     */
    public String getDescription() {
      return description;
    }
  }

  static final class RunFacets {
    private final NominalTimeRunFacet nominalTime;

    private final ParentRunFacet parent;

    @JsonAnySetter
    public final Map<String, CustomFacet> additionalProperties;

    /**
     * @param nominalTime
     * @param parent
     */
    @JsonCreator
    RunFacets(@JsonProperty("nominalTime") NominalTimeRunFacet nominalTime,
        @JsonProperty("parent") ParentRunFacet parent) {
      this.nominalTime = nominalTime;
      this.parent = parent;
      this.additionalProperties = new HashMap<>();
    }

    public NominalTimeRunFacet getNominalTime() {
      return nominalTime;
    }

    public ParentRunFacet getParent() {
      return parent;
    }

    /**
     * additional properties
     */
    @JsonAnyGetter
    public Map<String, CustomFacet> getAdditionalProperties() {
      return additionalProperties;
    }
  }

  static final class JobFacets {
    private final DocumentationJobFacet documentation;

    private final SourceCodeLocationJobFacet sourceCodeLocation;

    private final SQLJobFacet sql;

    @JsonAnySetter
    public final Map<String, CustomFacet> additionalProperties;

    /**
     * @param documentation
     * @param sourceCodeLocation
     * @param sql
     */
    @JsonCreator
    JobFacets(@JsonProperty("documentation") DocumentationJobFacet documentation,
        @JsonProperty("sourceCodeLocation") SourceCodeLocationJobFacet sourceCodeLocation,
        @JsonProperty("sql") SQLJobFacet sql) {
      this.documentation = documentation;
      this.sourceCodeLocation = sourceCodeLocation;
      this.sql = sql;
      this.additionalProperties = new HashMap<>();
    }

    public DocumentationJobFacet getDocumentation() {
      return documentation;
    }

    public SourceCodeLocationJobFacet getSourceCodeLocation() {
      return sourceCodeLocation;
    }

    public SQLJobFacet getSql() {
      return sql;
    }

    /**
     * additional properties
     */
    @JsonAnyGetter
    public Map<String, CustomFacet> getAdditionalProperties() {
      return additionalProperties;
    }
  }

  static final class Job {
    private final String namespace;

    private final String name;

    private final JobFacets facets;

    /**
     * @param namespace The namespace containing that job
     * @param name The unique name for that job within that namespace
     * @param facets The job facets.
     */
    @JsonCreator
    Job(@JsonProperty("namespace") String namespace, @JsonProperty("name") String name,
        @JsonProperty("facets") JobFacets facets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
    }

    /**
     * The namespace containing that job
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * The unique name for that job within that namespace
     */
    public String getName() {
      return name;
    }

    /**
     * The job facets.
     */
    public JobFacets getFacets() {
      return facets;
    }
  }

  static final class ParentRunFacetJob {
    private final String namespace;

    private final String name;

    /**
     * @param namespace The namespace containing that job
     * @param name The unique name for that job within that namespace
     */
    @JsonCreator
    ParentRunFacetJob(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name) {
      this.namespace = namespace;
      this.name = name;
    }

    /**
     * The namespace containing that job
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * The unique name for that job within that namespace
     */
    public String getName() {
      return name;
    }
  }

  static final class DocumentationDatasetFacet implements BaseFacet {
    private final String _producer;

    private final String _schemaURL;

    private final String description;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param description The description of the dataset.
     */
    @JsonCreator
    DocumentationDatasetFacet(@JsonProperty("_producer") String _producer,
        @JsonProperty("description") String description) {
      this._producer = _producer;
      this._schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json#/definitions/DocumentationDatasetFacet";
      this.description = description;
    }

    /**
     * URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    public String get_producer() {
      return _producer;
    }

    /**
     * The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     */
    public String get_schemaURL() {
      return _schemaURL;
    }

    /**
     * The description of the dataset.
     */
    public String getDescription() {
      return description;
    }
  }

  static final class SchemaDatasetFacetFields {
    private final String name;

    private final String type;

    private final String description;

    /**
     * @param name The name of the field.
     * @param type The type of the field.
     * @param description The description of the field.
     */
    @JsonCreator
    SchemaDatasetFacetFields(@JsonProperty("name") String name, @JsonProperty("type") String type,
        @JsonProperty("description") String description) {
      this.name = name;
      this.type = type;
      this.description = description;
    }

    /**
     * The name of the field.
     */
    public String getName() {
      return name;
    }

    /**
     * The type of the field.
     */
    public String getType() {
      return type;
    }

    /**
     * The description of the field.
     */
    public String getDescription() {
      return description;
    }
  }
}
