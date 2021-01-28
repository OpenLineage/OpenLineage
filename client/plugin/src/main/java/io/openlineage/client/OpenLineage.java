package io.openlineage.client;

import java.util.List;

public final class OpenLineage {

  static interface BaseFacet {
    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    String get_producer();
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    String get_schemaURL();
  }

  static final class SourceCodeLocationJobFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final String type;
    private final String url;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param type
     * @param url
     */
    public SourceCodeLocationJobFacet(
      String _producer,
      String _schemaURL,
      String type,
      String url
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.type=type;
      this.url=url;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    public String getType() { return type; }
    public String getUrl() { return url; }

  }

  static final class ParentRunFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final ParentRunFacetRun run;
    private final ParentRunFacetJob job;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param run
     * @param job
     */
    public ParentRunFacet(
      String _producer,
      String _schemaURL,
      ParentRunFacetRun run,
      ParentRunFacetJob job
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.run=run;
      this.job=job;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    public ParentRunFacetRun getRun() { return run; }
    public ParentRunFacetJob getJob() { return job; }

  }

  static final class DatasetFacets {

    private final DocumentationDatasetFacet documentation;
    private final SchemaDatasetFacet schema;
    private final DatasourceDatasetFacet dataSource;

    /**
     * @param documentation
     * @param schema
     * @param dataSource
     */
    public DatasetFacets(
      DocumentationDatasetFacet documentation,
      SchemaDatasetFacet schema,
      DatasourceDatasetFacet dataSource
    ) {
      this.documentation=documentation;
      this.schema=schema;
      this.dataSource=dataSource;
    }

    public DocumentationDatasetFacet getDocumentation() { return documentation; }
    public SchemaDatasetFacet getSchema() { return schema; }
    public DatasourceDatasetFacet getDataSource() { return dataSource; }

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
    public Dataset(
      String namespace,
      String name,
      DatasetFacets facets
    ) {
      this.namespace=namespace;
      this.name=name;
      this.facets=facets;
    }

    /** The namespace containing that dataset */
    public String getNamespace() { return namespace; }
    /** The unique name for that dataset within that namespace */
    public String getName() { return name; }
    /** The facets for this dataset */
    public DatasetFacets getFacets() { return facets; }

  }

  static final class NominalTimeRunFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final String nominalStartTime;
    private final String nominalEndTime;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param nominalStartTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time
     * @param nominalEndTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run)
     */
    public NominalTimeRunFacet(
      String _producer,
      String _schemaURL,
      String nominalStartTime,
      String nominalEndTime
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.nominalStartTime=nominalStartTime;
      this.nominalEndTime=nominalEndTime;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    /** An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time */
    public String getNominalStartTime() { return nominalStartTime; }
    /** An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run) */
    public String getNominalEndTime() { return nominalEndTime; }

  }

  static final class SchemaDatasetFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final List<SchemaDatasetFacetFields> fields;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param fields The fields of the table.
     */
    public SchemaDatasetFacet(
      String _producer,
      String _schemaURL,
      List<SchemaDatasetFacetFields> fields
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.fields=fields;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    /** The fields of the table. */
    public List<SchemaDatasetFacetFields> getFields() { return fields; }

  }

  static final class SQLJobFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final String query;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param query
     */
    public SQLJobFacet(
      String _producer,
      String _schemaURL,
      String query
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.query=query;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    public String getQuery() { return query; }

  }

  static final class Run {

    private final String runId;
    private final RunFacets facets;

    /**
     * @param runId The id of the run, unique relative to the job
     * @param facets The run facets.
     */
    public Run(
      String runId,
      RunFacets facets
    ) {
      this.runId=runId;
      this.facets=facets;
    }

    /** The id of the run, unique relative to the job */
    public String getRunId() { return runId; }
    /** The run facets. */
    public RunFacets getFacets() { return facets; }

  }

  static final class ParentRunFacetRun {

    private final String runId;

    /**
     * @param runId
     */
    public ParentRunFacetRun(
      String runId
    ) {
      this.runId=runId;
    }

    public String getRunId() { return runId; }

  }

  static final class DatasourceDatasetFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final String name;
    private final String uri;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param name
     * @param uri
     */
    public DatasourceDatasetFacet(
      String _producer,
      String _schemaURL,
      String name,
      String uri
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.name=name;
      this.uri=uri;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    public String getName() { return name; }
    public String getUri() { return uri; }

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
    public RunStateUpdate(
      String eventType,
      String eventTime,
      Run run,
      Job job,
      List<Dataset> inputs,
      List<Dataset> outputs,
      String producer
    ) {
      this.eventType=eventType;
      this.eventTime=eventTime;
      this.run=run;
      this.job=job;
      this.inputs=inputs;
      this.outputs=outputs;
      this.producer=producer;
    }

    /** the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete */
    public String getEventType() { return eventType; }
    /** the time the event occured at */
    public String getEventTime() { return eventTime; }
    public Run getRun() { return run; }
    public Job getJob() { return job; }
    /** The set of **input** datasets. */
    public List<Dataset> getInputs() { return inputs; }
    /** The set of **output** datasets. */
    public List<Dataset> getOutputs() { return outputs; }
    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String getProducer() { return producer; }

  }

  static final class DocumentationJobFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final String description;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param description The description of the job.
     */
    public DocumentationJobFacet(
      String _producer,
      String _schemaURL,
      String description
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.description=description;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    /** The description of the job. */
    public String getDescription() { return description; }

  }

  static final class RunFacets {

    private final NominalTimeRunFacet nominalTime;
    private final ParentRunFacet parent;

    /**
     * @param nominalTime
     * @param parent
     */
    public RunFacets(
      NominalTimeRunFacet nominalTime,
      ParentRunFacet parent
    ) {
      this.nominalTime=nominalTime;
      this.parent=parent;
    }

    public NominalTimeRunFacet getNominalTime() { return nominalTime; }
    public ParentRunFacet getParent() { return parent; }

  }

  static final class JobFacets {

    private final DocumentationJobFacet documentation;
    private final SourceCodeLocationJobFacet sourceCodeLocation;
    private final SQLJobFacet sql;

    /**
     * @param documentation
     * @param sourceCodeLocation
     * @param sql
     */
    public JobFacets(
      DocumentationJobFacet documentation,
      SourceCodeLocationJobFacet sourceCodeLocation,
      SQLJobFacet sql
    ) {
      this.documentation=documentation;
      this.sourceCodeLocation=sourceCodeLocation;
      this.sql=sql;
    }

    public DocumentationJobFacet getDocumentation() { return documentation; }
    public SourceCodeLocationJobFacet getSourceCodeLocation() { return sourceCodeLocation; }
    public SQLJobFacet getSql() { return sql; }

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
    public Job(
      String namespace,
      String name,
      JobFacets facets
    ) {
      this.namespace=namespace;
      this.name=name;
      this.facets=facets;
    }

    /** The namespace containing that job */
    public String getNamespace() { return namespace; }
    /** The unique name for that job within that namespace */
    public String getName() { return name; }
    /** The job facets. */
    public JobFacets getFacets() { return facets; }

  }

  static final class ParentRunFacetJob {

    private final String namespace;
    private final String name;

    /**
     * @param namespace The namespace containing that job
     * @param name The unique name for that job within that namespace
     */
    public ParentRunFacetJob(
      String namespace,
      String name
    ) {
      this.namespace=namespace;
      this.name=name;
    }

    /** The namespace containing that job */
    public String getNamespace() { return namespace; }
    /** The unique name for that job within that namespace */
    public String getName() { return name; }

  }

  static final class DocumentationDatasetFacet implements BaseFacet {

    private final String _producer;
    private final String _schemaURL;
    private final String description;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/)
     * @param description The description of the dataset.
     */
    public DocumentationDatasetFacet(
      String _producer,
      String _schemaURL,
      String description
    ) {
      this._producer=_producer;
      this._schemaURL=_schemaURL;
      this.description=description;
    }

    /** URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
    public String get_producer() { return _producer; }
    /** The URL to the corresponding version of the schema definition following a $ref URL Reference (see https://swagger.io/docs/specification/using-ref/) */
    public String get_schemaURL() { return _schemaURL; }
    /** The description of the dataset. */
    public String getDescription() { return description; }

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
    public SchemaDatasetFacetFields(
      String name,
      String type,
      String description
    ) {
      this.name=name;
      this.type=type;
      this.description=description;
    }

    /** The name of the field. */
    public String getName() { return name; }
    /** The type of the field. */
    public String getType() { return type; }
    /** The description of the field. */
    public String getDescription() { return description; }

  }
}
