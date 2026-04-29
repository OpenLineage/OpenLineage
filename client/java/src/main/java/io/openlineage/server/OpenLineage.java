/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.server;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.openlineage.server.OpenLineage.RunEvent.EventType;
import java.lang.Boolean;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class OpenLineage {
  public interface Builder<T> {
    /**
     * @return the constructed type
     */
    T build();
  }

  /**
   * model class for RunFacets
   */
  @JsonPropertyOrder
  public static final class RunFacets {
    @JsonAnySetter
    private final Map<String, RunFacet> additionalProperties;

    @JsonCreator
    public RunFacets() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, RunFacet> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RunFacets that = (RunFacets) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * model class for RunEvent
   */
  @JsonPropertyOrder({
      "eventTime",
      "producer",
      "schemaURL",
      "eventType",
      "run",
      "job",
      "inputs",
      "outputs"
  })
  public static final class RunEvent implements BaseEvent {
    private final ZonedDateTime eventTime;

    private final URI producer;

    private final URI schemaURL;

    private final RunEvent.EventType eventType;

    private final Run run;

    private final Job job;

    private final List<InputDataset> inputs;

    private final List<OutputDataset> outputs;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param eventTime the time the event occurred at
     * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent
     * @param eventType the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete
     * @param run the run
     * @param job the job
     * @param inputs The set of **input** datasets.
     * @param outputs The set of **output** datasets.
     */
    @JsonCreator
    public RunEvent(@JsonProperty("eventTime") ZonedDateTime eventTime,
        @JsonProperty("producer") URI producer, @JsonProperty("schemaURL") URI schemaURL,
        @JsonProperty("eventType") RunEvent.EventType eventType, @JsonProperty("run") Run run,
        @JsonProperty("job") Job job, @JsonProperty("inputs") List<InputDataset> inputs,
        @JsonProperty("outputs") List<OutputDataset> outputs) {
      this.eventTime = eventTime;
      this.producer = producer;
      this.schemaURL = schemaURL;
      this.eventType = eventType;
      this.run = run;
      this.job = job;
      this.inputs = inputs;
      this.outputs = outputs;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return the time the event occurred at
     */
    @Override
    public ZonedDateTime getEventTime() {
      return eventTime;
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI getProducer() {
      return producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent
     */
    @Override
    public URI getSchemaURL() {
      return schemaURL;
    }

    /**
     * @return the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete
     */
    public RunEvent.EventType getEventType() {
      return eventType;
    }

    public Run getRun() {
      return run;
    }

    public Job getJob() {
      return job;
    }

    /**
     * @return The set of **input** datasets.
     */
    public List<InputDataset> getInputs() {
      return inputs;
    }

    /**
     * @return The set of **output** datasets.
     */
    public List<OutputDataset> getOutputs() {
      return outputs;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RunEvent that = (RunEvent) o;
      if (!Objects.equals(eventTime, that.eventTime)) return false;
      if (!Objects.equals(schemaURL, that.schemaURL)) return false;
      if (!Objects.equals(eventType, that.eventType)) return false;
      if (!Objects.equals(run, that.run)) return false;
      if (!Objects.equals(job, that.job)) return false;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(outputs, that.outputs)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(eventTime, schemaURL, eventType, run, job, inputs, outputs, additionalProperties);
    }

    public enum EventType {
      START,

      RUNNING,

      COMPLETE,

      ABORT,

      FAIL,

      OTHER
    }
  }

  /**
   * Interface for BaseEvent
   */
  public interface BaseEvent {
    /**
     * @return the time the event occurred at
     */
    ZonedDateTime getEventTime();

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    URI getProducer();

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent
     */
    URI getSchemaURL();

    /**
     * @return additional properties
     */
    Map<String, Object> getAdditionalProperties();
  }

  /**
   * model class for JobFacets
   */
  @JsonPropertyOrder
  public static final class JobFacets {
    @JsonAnySetter
    private final Map<String, JobFacet> additionalProperties;

    @JsonCreator
    public JobFacets() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, JobFacet> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JobFacets that = (JobFacets) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * model class for InputDatasetInputFacets
   */
  @JsonPropertyOrder
  public static final class InputDatasetInputFacets {
    @JsonAnySetter
    private final Map<String, InputDatasetFacet> additionalProperties;

    @JsonCreator
    public InputDatasetInputFacets() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, InputDatasetFacet> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InputDatasetInputFacets that = (InputDatasetInputFacets) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * model class for DatasetFacet
   */
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted"
  })
  public static final class DatasetFacet implements BaseFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @JsonCreator
    public DatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("_schemaURL") URI _schemaURL) {
      this._producer = _producer;
      this._schemaURL = _schemaURL;
      this._deleted = null;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI get_producer() {
      return _producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @Override
    public URI get_schemaURL() {
      return _schemaURL;
    }

    /**
     * @return set to true to delete a facet
     */
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DatasetFacet that = (DatasetFacet) o;
      if (!Objects.equals(_schemaURL, that._schemaURL)) return false;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_schemaURL, _deleted, additionalProperties);
    }
  }

  /**
   * model class for OutputDatasetFacet
   */
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL"
  })
  public static final class OutputDatasetFacet implements BaseFacet {
    private final URI _producer;

    private final URI _schemaURL;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @JsonCreator
    public OutputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("_schemaURL") URI _schemaURL) {
      this._producer = _producer;
      this._schemaURL = _schemaURL;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI get_producer() {
      return _producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @Override
    public URI get_schemaURL() {
      return _schemaURL;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OutputDatasetFacet that = (OutputDatasetFacet) o;
      if (!Objects.equals(_schemaURL, that._schemaURL)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_schemaURL, additionalProperties);
    }
  }

  /**
   * model class for DatasetEvent
   */
  @JsonPropertyOrder({
      "eventTime",
      "producer",
      "schemaURL",
      "dataset"
  })
  public static final class DatasetEvent implements BaseEvent {
    private final ZonedDateTime eventTime;

    private final URI producer;

    private final URI schemaURL;

    private final StaticDataset dataset;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param eventTime the time the event occurred at
     * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent
     * @param dataset the dataset
     */
    @JsonCreator
    public DatasetEvent(@JsonProperty("eventTime") ZonedDateTime eventTime,
        @JsonProperty("producer") URI producer, @JsonProperty("schemaURL") URI schemaURL,
        @JsonProperty("dataset") StaticDataset dataset) {
      this.eventTime = eventTime;
      this.producer = producer;
      this.schemaURL = schemaURL;
      this.dataset = dataset;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return the time the event occurred at
     */
    @Override
    public ZonedDateTime getEventTime() {
      return eventTime;
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI getProducer() {
      return producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent
     */
    @Override
    public URI getSchemaURL() {
      return schemaURL;
    }

    public StaticDataset getDataset() {
      return dataset;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DatasetEvent that = (DatasetEvent) o;
      if (!Objects.equals(eventTime, that.eventTime)) return false;
      if (!Objects.equals(schemaURL, that.schemaURL)) return false;
      if (!Objects.equals(dataset, that.dataset)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(eventTime, schemaURL, dataset, additionalProperties);
    }
  }

  /**
   * model class for StaticDataset
   */
  @JsonPropertyOrder({
      "namespace",
      "name",
      "facets"
  })
  public static final class StaticDataset implements Dataset {
    private final String namespace;

    private final String name;

    private final DatasetFacets facets;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace containing that dataset
     * @param name The unique name for that dataset within that namespace
     * @param facets The facets for this dataset
     */
    @JsonCreator
    public StaticDataset(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("facets") DatasetFacets facets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace containing that dataset
     */
    @Override
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The unique name for that dataset within that namespace
     */
    @Override
    public String getName() {
      return name;
    }

    /**
     * @return The facets for this dataset
     */
    @Override
    public DatasetFacets getFacets() {
      return facets;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StaticDataset that = (StaticDataset) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets, additionalProperties);
    }
  }

  /**
   * model class for DatasetFacets
   */
  @JsonPropertyOrder
  public static final class DatasetFacets {
    @JsonAnySetter
    private final Map<String, DatasetFacet> additionalProperties;

    @JsonCreator
    public DatasetFacets() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, DatasetFacet> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DatasetFacets that = (DatasetFacets) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * model class for Run
   */
  @JsonPropertyOrder({
      "runId",
      "facets"
  })
  public static final class Run {
    private final UUID runId;

    private final RunFacets facets;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param runId The globally unique ID of the run associated with the job.
     * @param facets The run facets.
     */
    @JsonCreator
    public Run(@JsonProperty("runId") UUID runId, @JsonProperty("facets") RunFacets facets) {
      this.runId = runId;
      this.facets = facets;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The globally unique ID of the run associated with the job.
     */
    public UUID getRunId() {
      return runId;
    }

    /**
     * @return The run facets.
     */
    public RunFacets getFacets() {
      return facets;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Run that = (Run) o;
      if (!Objects.equals(runId, that.runId)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(runId, facets, additionalProperties);
    }
  }

  /**
   * Interface for Dataset
   */
  public interface Dataset {
    /**
     * @return The namespace containing that dataset
     */
    String getNamespace();

    /**
     * @return The unique name for that dataset within that namespace
     */
    String getName();

    /**
     * @return The facets for this dataset
     */
    DatasetFacets getFacets();

    /**
     * @return additional properties
     */
    Map<String, Object> getAdditionalProperties();
  }

  /**
   * model class for OutputDatasetOutputFacets
   */
  @JsonPropertyOrder
  public static final class OutputDatasetOutputFacets {
    @JsonAnySetter
    private final Map<String, OutputDatasetFacet> additionalProperties;

    @JsonCreator
    public OutputDatasetOutputFacets() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, OutputDatasetFacet> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OutputDatasetOutputFacets that = (OutputDatasetOutputFacets) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * model class for RunFacet
   */
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL"
  })
  public static final class RunFacet implements BaseFacet {
    private final URI _producer;

    private final URI _schemaURL;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @JsonCreator
    public RunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("_schemaURL") URI _schemaURL) {
      this._producer = _producer;
      this._schemaURL = _schemaURL;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI get_producer() {
      return _producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @Override
    public URI get_schemaURL() {
      return _schemaURL;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RunFacet that = (RunFacet) o;
      if (!Objects.equals(_schemaURL, that._schemaURL)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_schemaURL, additionalProperties);
    }
  }

  /**
   * model class for OutputDataset
   */
  @JsonPropertyOrder({
      "namespace",
      "name",
      "facets",
      "outputFacets"
  })
  public static final class OutputDataset implements Dataset {
    private final String namespace;

    private final String name;

    private final DatasetFacets facets;

    private final OutputDatasetOutputFacets outputFacets;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace containing that dataset
     * @param name The unique name for that dataset within that namespace
     * @param facets The facets for this dataset
     * @param outputFacets The output facets for this dataset
     */
    @JsonCreator
    public OutputDataset(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("facets") DatasetFacets facets,
        @JsonProperty("outputFacets") OutputDatasetOutputFacets outputFacets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
      this.outputFacets = outputFacets;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace containing that dataset
     */
    @Override
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The unique name for that dataset within that namespace
     */
    @Override
    public String getName() {
      return name;
    }

    /**
     * @return The facets for this dataset
     */
    @Override
    public DatasetFacets getFacets() {
      return facets;
    }

    /**
     * @return The output facets for this dataset
     */
    public OutputDatasetOutputFacets getOutputFacets() {
      return outputFacets;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OutputDataset that = (OutputDataset) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      if (!Objects.equals(outputFacets, that.outputFacets)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets, outputFacets, additionalProperties);
    }
  }

  /**
   * Interface for BaseFacet
   */
  public interface BaseFacet {
    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    URI get_producer();

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    URI get_schemaURL();

    /**
     * @return additional properties
     */
    Map<String, Object> getAdditionalProperties();
  }

  /**
   * model class for InputDataset
   */
  @JsonPropertyOrder({
      "namespace",
      "name",
      "facets",
      "inputFacets"
  })
  public static final class InputDataset implements Dataset {
    private final String namespace;

    private final String name;

    private final DatasetFacets facets;

    private final InputDatasetInputFacets inputFacets;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace containing that dataset
     * @param name The unique name for that dataset within that namespace
     * @param facets The facets for this dataset
     * @param inputFacets The input facets for this dataset.
     */
    @JsonCreator
    public InputDataset(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("facets") DatasetFacets facets,
        @JsonProperty("inputFacets") InputDatasetInputFacets inputFacets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
      this.inputFacets = inputFacets;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace containing that dataset
     */
    @Override
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The unique name for that dataset within that namespace
     */
    @Override
    public String getName() {
      return name;
    }

    /**
     * @return The facets for this dataset
     */
    @Override
    public DatasetFacets getFacets() {
      return facets;
    }

    /**
     * @return The input facets for this dataset.
     */
    public InputDatasetInputFacets getInputFacets() {
      return inputFacets;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InputDataset that = (InputDataset) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      if (!Objects.equals(inputFacets, that.inputFacets)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets, inputFacets, additionalProperties);
    }
  }

  /**
   * model class for Job
   */
  @JsonPropertyOrder({
      "namespace",
      "name",
      "facets"
  })
  public static final class Job {
    private final String namespace;

    private final String name;

    private final JobFacets facets;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace containing that job
     * @param name The unique name for that job within that namespace
     * @param facets The job facets.
     */
    @JsonCreator
    public Job(@JsonProperty("namespace") String namespace, @JsonProperty("name") String name,
        @JsonProperty("facets") JobFacets facets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace containing that job
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The unique name for that job within that namespace
     */
    public String getName() {
      return name;
    }

    /**
     * @return The job facets.
     */
    public JobFacets getFacets() {
      return facets;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Job that = (Job) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets, additionalProperties);
    }
  }

  /**
   * model class for InputDatasetFacet
   */
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL"
  })
  public static final class InputDatasetFacet implements BaseFacet {
    private final URI _producer;

    private final URI _schemaURL;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @JsonCreator
    public InputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("_schemaURL") URI _schemaURL) {
      this._producer = _producer;
      this._schemaURL = _schemaURL;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI get_producer() {
      return _producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @Override
    public URI get_schemaURL() {
      return _schemaURL;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InputDatasetFacet that = (InputDatasetFacet) o;
      if (!Objects.equals(_schemaURL, that._schemaURL)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_schemaURL, additionalProperties);
    }
  }

  /**
   * model class for JobEvent
   */
  @JsonPropertyOrder({
      "eventTime",
      "producer",
      "schemaURL",
      "job",
      "inputs",
      "outputs"
  })
  public static final class JobEvent implements BaseEvent {
    private final ZonedDateTime eventTime;

    private final URI producer;

    private final URI schemaURL;

    private final Job job;

    private final List<InputDataset> inputs;

    private final List<OutputDataset> outputs;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param eventTime the time the event occurred at
     * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent
     * @param job the job
     * @param inputs The set of **input** datasets.
     * @param outputs The set of **output** datasets.
     */
    @JsonCreator
    public JobEvent(@JsonProperty("eventTime") ZonedDateTime eventTime,
        @JsonProperty("producer") URI producer, @JsonProperty("schemaURL") URI schemaURL,
        @JsonProperty("job") Job job, @JsonProperty("inputs") List<InputDataset> inputs,
        @JsonProperty("outputs") List<OutputDataset> outputs) {
      this.eventTime = eventTime;
      this.producer = producer;
      this.schemaURL = schemaURL;
      this.job = job;
      this.inputs = inputs;
      this.outputs = outputs;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return the time the event occurred at
     */
    @Override
    public ZonedDateTime getEventTime() {
      return eventTime;
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI getProducer() {
      return producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent
     */
    @Override
    public URI getSchemaURL() {
      return schemaURL;
    }

    public Job getJob() {
      return job;
    }

    /**
     * @return The set of **input** datasets.
     */
    public List<InputDataset> getInputs() {
      return inputs;
    }

    /**
     * @return The set of **output** datasets.
     */
    public List<OutputDataset> getOutputs() {
      return outputs;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JobEvent that = (JobEvent) o;
      if (!Objects.equals(eventTime, that.eventTime)) return false;
      if (!Objects.equals(schemaURL, that.schemaURL)) return false;
      if (!Objects.equals(job, that.job)) return false;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(outputs, that.outputs)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(eventTime, schemaURL, job, inputs, outputs, additionalProperties);
    }
  }

  /**
   * model class for JobFacet
   */
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted"
  })
  public static final class JobFacet implements BaseFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @JsonCreator
    public JobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("_schemaURL") URI _schemaURL) {
      this._producer = _producer;
      this._schemaURL = _schemaURL;
      this._deleted = null;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @Override
    public URI get_producer() {
      return _producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    @Override
    public URI get_schemaURL() {
      return _schemaURL;
    }

    /**
     * @return set to true to delete a facet
     */
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /**
     * Get object with additional properties
     */
    void withAdditionalProperties() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JobFacet that = (JobFacet) o;
      if (!Objects.equals(_schemaURL, that._schemaURL)) return false;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_schemaURL, _deleted, additionalProperties);
    }
  }
}
