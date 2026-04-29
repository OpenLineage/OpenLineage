/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import java.lang.Boolean;
import java.lang.Double;
import java.lang.Long;
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

/**
 * Usage:
 * <pre>{@code
 *   URI producer = URI.create("http://my.producer/uri");
 *   OpenLineage ol = new OpenLineage(producer);
 *   UUID runId = UUID.randomUUID();
 *   RunFacets runFacets =
 *     ol.newRunFacetsBuilder().nominalTime(ol.newNominalTimeRunFacet(now, now)).build();
 *   Run run = ol.newRun(runId, runFacets);
 *   String name = "jobName";
 *   String namespace = "namespace";
 *   JobFacets jobFacets = ol.newJobFacetsBuilder().build();
 *   Job job = ol.newJob(namespace, name, jobFacets);
 *   List<InputDataset> inputs = Arrays.asList(ol.newInputDataset("ins", "input", null, null));
 *   List<OutputDataset> outputs = Arrays.asList(ol.newOutputDataset("ons", "output", null, null));
 *   RunEvent runStateUpdate =
 *     ol.newRunEvent(now, OpenLineage.RunEvent.EventType.START, run, job, inputs, outputs);
 * }</pre>
 */
public final class OpenLineage {
  private final URI producer;

  /**
   * Starting point to create OpenLineage objects. Use the OpenLineage instance to create events and facets
   * @param producer the identifier of the library using the client to generate OpenLineage events
   */
  public OpenLineage(URI producer) {
    this.producer = producer;
  }

  /**
   * Factory method for SchemaDatasetFacetFields
   *
   * @param name The name of the field.
   * @param type The type of the field.
   * @param description The description of the field.
   * @param ordinal_position The ordinal position of the field in the schema (1-indexed).
   * @param fields Nested struct fields.
   * @return SchemaDatasetFacetFields
   */
  public SchemaDatasetFacetFields newSchemaDatasetFacetFields(String name, String type,
      String description, Long ordinal_position, List<SchemaDatasetFacetFields> fields) {
    return new SchemaDatasetFacetFields(name, type, description, ordinal_position, fields);
  }

  /**
   * Creates a builder for SchemaDatasetFacetFields
   * @return a new builder for SchemaDatasetFacetFields
   */
  public SchemaDatasetFacetFieldsBuilder newSchemaDatasetFacetFieldsBuilder() {
    return new SchemaDatasetFacetFieldsBuilder();
  }

  /**
   * Factory method for LineageDatasetInput
   *
   * @param namespace The namespace of the source entity.
   * @param name The name of the source entity.
   * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
   * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
   * @param transformations Transformations applied to the source data.
   * @return LineageDatasetInput
   */
  public LineageDatasetInput newLineageDatasetInput(String namespace, String name, String type,
      String field, List<LineageDatasetTransformation> transformations) {
    return new LineageDatasetInput(namespace, name, type, field, transformations);
  }

  /**
   * Creates a builder for LineageDatasetInput
   * @return a new builder for LineageDatasetInput
   */
  public LineageDatasetInputBuilder newLineageDatasetInputBuilder() {
    return new LineageDatasetInputBuilder();
  }

  /**
   * Factory method for ParentRunFacet
   *
   * @param run the run
   * @param job the job
   * @param root the root
   * @return ParentRunFacet
   */
  public ParentRunFacet newParentRunFacet(ParentRunFacetRun run, ParentRunFacetJob job,
      ParentRunFacetRoot root) {
    return new ParentRunFacet(this.producer, run, job, root);
  }

  /**
   * Creates a builder for ParentRunFacet
   * @return a new builder for ParentRunFacet
   */
  public ParentRunFacetBuilder newParentRunFacetBuilder() {
    return new ParentRunFacetBuilder();
  }

  /**
   * Factory method for OutputStatisticsOutputDatasetFacet
   *
   * @param rowCount The number of rows written to the dataset
   * @param size The size in bytes written to the dataset
   * @param fileCount The number of files written to the dataset
   * @return OutputStatisticsOutputDatasetFacet
   */
  public OutputStatisticsOutputDatasetFacet newOutputStatisticsOutputDatasetFacet(Long rowCount,
      Long size, Long fileCount) {
    return new OutputStatisticsOutputDatasetFacet(this.producer, rowCount, size, fileCount);
  }

  /**
   * Creates a builder for OutputStatisticsOutputDatasetFacet
   * @return a new builder for OutputStatisticsOutputDatasetFacet
   */
  public OutputStatisticsOutputDatasetFacetBuilder newOutputStatisticsOutputDatasetFacetBuilder() {
    return new OutputStatisticsOutputDatasetFacetBuilder();
  }

  /**
   * Factory method for InputSubsetInputDatasetFacet
   *
   * @param inputCondition the inputCondition
   * @return InputSubsetInputDatasetFacet
   */
  public InputSubsetInputDatasetFacet newInputSubsetInputDatasetFacet(
      LocationSubsetCondition inputCondition) {
    return new InputSubsetInputDatasetFacet(this.producer, inputCondition);
  }

  /**
   * Creates a builder for InputSubsetInputDatasetFacet
   * @return a new builder for InputSubsetInputDatasetFacet
   */
  public InputSubsetInputDatasetFacetBuilder newInputSubsetInputDatasetFacetBuilder() {
    return new InputSubsetInputDatasetFacetBuilder();
  }

  /**
   * Factory method for ExtractionErrorRunFacetErrors
   *
   * @param errorMessage Text representation of extraction error message.
   * @param stackTrace Stack trace of extraction error message
   * @param task Text representation of task that failed. This can be, for example, SQL statement that parser could not interpret.
   * @param taskNumber Order of task (counted from 0).
   * @return ExtractionErrorRunFacetErrors
   */
  public ExtractionErrorRunFacetErrors newExtractionErrorRunFacetErrors(String errorMessage,
      String stackTrace, String task, Long taskNumber) {
    return new ExtractionErrorRunFacetErrors(errorMessage, stackTrace, task, taskNumber);
  }

  /**
   * Creates a builder for ExtractionErrorRunFacetErrors
   * @return a new builder for ExtractionErrorRunFacetErrors
   */
  public ExtractionErrorRunFacetErrorsBuilder newExtractionErrorRunFacetErrorsBuilder() {
    return new ExtractionErrorRunFacetErrorsBuilder();
  }

  /**
   * Factory method for JobTypeJobFacet
   *
   * @param processingType Job processing type like: BATCH or STREAMING
   * @param integration OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK
   * @param jobType Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific field.
   * @return JobTypeJobFacet
   */
  public JobTypeJobFacet newJobTypeJobFacet(String processingType, String integration,
      String jobType) {
    return new JobTypeJobFacet(this.producer, processingType, integration, jobType);
  }

  /**
   * Creates a builder for JobTypeJobFacet
   * @return a new builder for JobTypeJobFacet
   */
  public JobTypeJobFacetBuilder newJobTypeJobFacetBuilder() {
    return new JobTypeJobFacetBuilder();
  }

  /**
   * Factory method for Run
   *
   * @param runId The globally unique ID of the run associated with the job.
   * @param facets The run facets.
   * @return Run
   */
  public Run newRun(UUID runId, RunFacets facets) {
    return new Run(runId, facets);
  }

  /**
   * Creates a builder for Run
   * @return a new builder for Run
   */
  public RunBuilder newRunBuilder() {
    return new RunBuilder();
  }

  /**
   * Factory method for DataQualityMetricsDatasetFacetColumnMetrics
   *
   * @return DataQualityMetricsDatasetFacetColumnMetrics
   */
  public DataQualityMetricsDatasetFacetColumnMetrics newDataQualityMetricsDatasetFacetColumnMetrics(
      ) {
    return new DataQualityMetricsDatasetFacetColumnMetrics();
  }

  /**
   * Creates a builder for DataQualityMetricsDatasetFacetColumnMetrics
   * @return a new builder for DataQualityMetricsDatasetFacetColumnMetrics
   */
  public DataQualityMetricsDatasetFacetColumnMetricsBuilder newDataQualityMetricsDatasetFacetColumnMetricsBuilder(
      ) {
    return new DataQualityMetricsDatasetFacetColumnMetricsBuilder();
  }

  /**
   * Factory method for ParentRunFacetRoot
   *
   * @param run the run
   * @param job the job
   * @return ParentRunFacetRoot
   */
  public ParentRunFacetRoot newParentRunFacetRoot(RootRun run, RootJob job) {
    return new ParentRunFacetRoot(run, job);
  }

  /**
   * Creates a builder for ParentRunFacetRoot
   * @return a new builder for ParentRunFacetRoot
   */
  public ParentRunFacetRootBuilder newParentRunFacetRootBuilder() {
    return new ParentRunFacetRootBuilder();
  }

  /**
   * Factory method for DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles
   *
   * @return DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles
   */
  public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles(
      ) {
    return new DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles();
  }

  /**
   * Creates a builder for DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles
   * @return a new builder for DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles
   */
  public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder(
      ) {
    return new DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder();
  }

  /**
   * Factory method for ExternalQueryRunFacet
   *
   * @param externalQueryId Identifier for the external system
   * @param source source of the external query
   * @return ExternalQueryRunFacet
   */
  public ExternalQueryRunFacet newExternalQueryRunFacet(String externalQueryId, String source) {
    return new ExternalQueryRunFacet(this.producer, externalQueryId, source);
  }

  /**
   * Creates a builder for ExternalQueryRunFacet
   * @return a new builder for ExternalQueryRunFacet
   */
  public ExternalQueryRunFacetBuilder newExternalQueryRunFacetBuilder() {
    return new ExternalQueryRunFacetBuilder();
  }

  /**
   * Factory method for OutputDatasetOutputFacets
   *
   * @param outputStatistics the outputStatistics
   * @param icebergCommitReport the icebergCommitReport
   * @return OutputDatasetOutputFacets
   */
  public OutputDatasetOutputFacets newOutputDatasetOutputFacets(
      OutputStatisticsOutputDatasetFacet outputStatistics,
      IcebergCommitReportOutputDatasetFacet icebergCommitReport) {
    return new OutputDatasetOutputFacets(outputStatistics, icebergCommitReport);
  }

  /**
   * Creates a builder for OutputDatasetOutputFacets
   * @return a new builder for OutputDatasetOutputFacets
   */
  public OutputDatasetOutputFacetsBuilder newOutputDatasetOutputFacetsBuilder() {
    return new OutputDatasetOutputFacetsBuilder();
  }

  /**
   * Factory method for NominalTimeRunFacet
   *
   * @param nominalStartTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time
   * @param nominalEndTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run)
   * @return NominalTimeRunFacet
   */
  public NominalTimeRunFacet newNominalTimeRunFacet(ZonedDateTime nominalStartTime,
      ZonedDateTime nominalEndTime) {
    return new NominalTimeRunFacet(this.producer, nominalStartTime, nominalEndTime);
  }

  /**
   * Creates a builder for NominalTimeRunFacet
   * @return a new builder for NominalTimeRunFacet
   */
  public NominalTimeRunFacetBuilder newNominalTimeRunFacetBuilder() {
    return new NominalTimeRunFacetBuilder();
  }

  /**
   * Factory method for LiteralCompareExpression
   *
   * @param value the value
   * @return LiteralCompareExpression
   */
  public LiteralCompareExpression newLiteralCompareExpression(String value) {
    return new LiteralCompareExpression(value);
  }

  /**
   * Creates a builder for LiteralCompareExpression
   * @return a new builder for LiteralCompareExpression
   */
  public LiteralCompareExpressionBuilder newLiteralCompareExpressionBuilder() {
    return new LiteralCompareExpressionBuilder();
  }

  /**
   * Factory method for OutputSubsetOutputDatasetFacet
   *
   * @param outputCondition the outputCondition
   * @return OutputSubsetOutputDatasetFacet
   */
  public OutputSubsetOutputDatasetFacet newOutputSubsetOutputDatasetFacet(
      LocationSubsetCondition outputCondition) {
    return new OutputSubsetOutputDatasetFacet(this.producer, outputCondition);
  }

  /**
   * Creates a builder for OutputSubsetOutputDatasetFacet
   * @return a new builder for OutputSubsetOutputDatasetFacet
   */
  public OutputSubsetOutputDatasetFacetBuilder newOutputSubsetOutputDatasetFacetBuilder() {
    return new OutputSubsetOutputDatasetFacetBuilder();
  }

  /**
   * @return InputDatasetFacet
   */
  public InputDatasetFacet newInputDatasetFacet() {
    return new DefaultInputDatasetFacet(this.producer);
  }

  /**
   * Factory method for ColumnLineageDatasetFacetFieldsAdditional
   *
   * @param inputFields the inputFields
   * @param transformationDescription a string representation of the transformation applied
   * @param transformationType IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input; MASKED: no original data available (like a hash of PII for example)
   * @return ColumnLineageDatasetFacetFieldsAdditional
   */
  public ColumnLineageDatasetFacetFieldsAdditional newColumnLineageDatasetFacetFieldsAdditional(
      List<InputField> inputFields, String transformationDescription, String transformationType) {
    return new ColumnLineageDatasetFacetFieldsAdditional(inputFields, transformationDescription, transformationType);
  }

  /**
   * Creates a builder for ColumnLineageDatasetFacetFieldsAdditional
   * @return a new builder for ColumnLineageDatasetFacetFieldsAdditional
   */
  public ColumnLineageDatasetFacetFieldsAdditionalBuilder newColumnLineageDatasetFacetFieldsAdditionalBuilder(
      ) {
    return new ColumnLineageDatasetFacetFieldsAdditionalBuilder();
  }

  /**
   * Factory method for LineageRunFacet
   *
   * @param lineage Lineage entries describing data flow observed during this run. Each entry identifies a target entity and the sources that feed into it.
   * @return LineageRunFacet
   */
  public LineageRunFacet newLineageRunFacet(List<LineageEntry> lineage) {
    return new LineageRunFacet(this.producer, lineage);
  }

  /**
   * Creates a builder for LineageRunFacet
   * @return a new builder for LineageRunFacet
   */
  public LineageRunFacetBuilder newLineageRunFacetBuilder() {
    return new LineageRunFacetBuilder();
  }

  /**
   * Factory method for DataQualityMetricsInputDatasetFacetColumnMetricsAdditional
   *
   * @param nullCount The number of null values in this column for the rows evaluated
   * @param distinctCount The number of distinct values in this column for the rows evaluated
   * @param sum The total sum of values in this column for the rows evaluated
   * @param count The number of values in this column
   * @param min the min
   * @param max the max
   * @param quantiles The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
   * @return DataQualityMetricsInputDatasetFacetColumnMetricsAdditional
   */
  public DataQualityMetricsInputDatasetFacetColumnMetricsAdditional newDataQualityMetricsInputDatasetFacetColumnMetricsAdditional(
      Long nullCount, Long distinctCount, Double sum, Double count, Double min, Double max,
      DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles quantiles) {
    return new DataQualityMetricsInputDatasetFacetColumnMetricsAdditional(nullCount, distinctCount, sum, count, min, max, quantiles);
  }

  /**
   * Creates a builder for DataQualityMetricsInputDatasetFacetColumnMetricsAdditional
   * @return a new builder for DataQualityMetricsInputDatasetFacetColumnMetricsAdditional
   */
  public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder(
      ) {
    return new DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder();
  }

  /**
   * Factory method for DataQualityMetricsInputDatasetFacet
   *
   * @param rowCount The number of rows evaluated
   * @param bytes The size in bytes
   * @param fileCount The number of files evaluated
   * @param lastUpdated The last time the dataset was changed
   * @param columnMetrics The property key is the column name
   * @return DataQualityMetricsInputDatasetFacet
   */
  public DataQualityMetricsInputDatasetFacet newDataQualityMetricsInputDatasetFacet(Long rowCount,
      Long bytes, Long fileCount, ZonedDateTime lastUpdated,
      DataQualityMetricsInputDatasetFacetColumnMetrics columnMetrics) {
    return new DataQualityMetricsInputDatasetFacet(this.producer, rowCount, bytes, fileCount, lastUpdated, columnMetrics);
  }

  /**
   * Creates a builder for DataQualityMetricsInputDatasetFacet
   * @return a new builder for DataQualityMetricsInputDatasetFacet
   */
  public DataQualityMetricsInputDatasetFacetBuilder newDataQualityMetricsInputDatasetFacetBuilder(
      ) {
    return new DataQualityMetricsInputDatasetFacetBuilder();
  }

  /**
   * Factory method for DocumentationJobFacet
   *
   * @param description The description of the job.
   * @param contentType MIME type of the description field content.
   * @return DocumentationJobFacet
   */
  public DocumentationJobFacet newDocumentationJobFacet(String description, String contentType) {
    return new DocumentationJobFacet(this.producer, description, contentType);
  }

  /**
   * Creates a builder for DocumentationJobFacet
   * @return a new builder for DocumentationJobFacet
   */
  public DocumentationJobFacetBuilder newDocumentationJobFacetBuilder() {
    return new DocumentationJobFacetBuilder();
  }

  /**
   * Factory method for LineageTransformation
   *
   * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
   * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
   * @param description A string representation of the transformation applied.
   * @param masking Whether the transformation masks the data (e.g., hashing PII).
   * @return LineageTransformation
   */
  public LineageTransformation newLineageTransformation(String type, String subtype,
      String description, Boolean masking) {
    return new LineageTransformation(type, subtype, description, masking);
  }

  /**
   * Creates a builder for LineageTransformation
   * @return a new builder for LineageTransformation
   */
  public LineageTransformationBuilder newLineageTransformationBuilder() {
    return new LineageTransformationBuilder();
  }

  /**
   * Factory method for InputDatasetInputFacets
   *
   * @param dataQualityAssertions the dataQualityAssertions
   * @param inputStatistics the inputStatistics
   * @param dataQualityMetrics the dataQualityMetrics
   * @param subset the subset
   * @param icebergScanReport the icebergScanReport
   * @return InputDatasetInputFacets
   */
  public InputDatasetInputFacets newInputDatasetInputFacets(
      DataQualityAssertionsDatasetFacet dataQualityAssertions,
      InputStatisticsInputDatasetFacet inputStatistics,
      DataQualityMetricsInputDatasetFacet dataQualityMetrics, InputSubsetInputDatasetFacet subset,
      IcebergScanReportInputDatasetFacet icebergScanReport) {
    return new InputDatasetInputFacets(dataQualityAssertions, inputStatistics, dataQualityMetrics, subset, icebergScanReport);
  }

  /**
   * Creates a builder for InputDatasetInputFacets
   * @return a new builder for InputDatasetInputFacets
   */
  public InputDatasetInputFacetsBuilder newInputDatasetInputFacetsBuilder() {
    return new InputDatasetInputFacetsBuilder();
  }

  /**
   * Factory method for LineageDatasetFacet
   *
   * @param inputs Dataset-level source inputs that feed into this dataset. When a source includes a 'field' property, it represents a dataset-wide operation (e.g., GROUP BY, FILTER) where that source column affects the entire target dataset.
   * @param fields Column-level lineage. Maps target field names in this dataset to their source inputs.
   * @return LineageDatasetFacet
   */
  public LineageDatasetFacet newLineageDatasetFacet(List<LineageDatasetInput> inputs,
      LineageDatasetFacetFields fields) {
    return new LineageDatasetFacet(this.producer, inputs, fields);
  }

  /**
   * Creates a builder for LineageDatasetFacet
   * @return a new builder for LineageDatasetFacet
   */
  public LineageDatasetFacetBuilder newLineageDatasetFacetBuilder() {
    return new LineageDatasetFacetBuilder();
  }

  /**
   * Factory method for LineageInput
   *
   * @param namespace The namespace of the source entity.
   * @param name The name of the source entity.
   * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
   * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
   * @param transformations Transformations applied to the source data.
   * @return LineageInput
   */
  public LineageInput newLineageInput(String namespace, String name, String type, String field,
      List<LineageTransformation> transformations) {
    return new LineageInput(namespace, name, type, field, transformations);
  }

  /**
   * Creates a builder for LineageInput
   * @return a new builder for LineageInput
   */
  public LineageInputBuilder newLineageInputBuilder() {
    return new LineageInputBuilder();
  }

  /**
   * Factory method for SQLJobFacet
   *
   * @param query the query
   * @param dialect the dialect
   * @return SQLJobFacet
   */
  public SQLJobFacet newSQLJobFacet(String query, String dialect) {
    return new SQLJobFacet(this.producer, query, dialect);
  }

  /**
   * Creates a builder for SQLJobFacet
   * @return a new builder for SQLJobFacet
   */
  public SQLJobFacetBuilder newSQLJobFacetBuilder() {
    return new SQLJobFacetBuilder();
  }

  /**
   * Factory method for LineageDatasetFacetFields
   *
   * @return LineageDatasetFacetFields
   */
  public LineageDatasetFacetFields newLineageDatasetFacetFields() {
    return new LineageDatasetFacetFields();
  }

  /**
   * Creates a builder for LineageDatasetFacetFields
   * @return a new builder for LineageDatasetFacetFields
   */
  public LineageDatasetFacetFieldsBuilder newLineageDatasetFacetFieldsBuilder() {
    return new LineageDatasetFacetFieldsBuilder();
  }

  /**
   * Factory method for GcpComposerJobFacet
   *
   * @param environmentName Cloud Composer Environment name
   * @param dagId The id of the DAG
   * @param operator Operator class name. Only present for tasks, not for DAGs. For example `PythonOperator`
   * @param taskId The id of the task. Only present for tasks, not for DAGs
   * @param airflowVersion Version of Airflow, suffixed by `+composer`
   * @param composerVersion Version of the Cloud Composer environment
   * @return GcpComposerJobFacet
   */
  public GcpComposerJobFacet newGcpComposerJobFacet(String environmentName, String dagId,
      String operator, String taskId, String airflowVersion, String composerVersion) {
    return new GcpComposerJobFacet(this.producer, environmentName, dagId, operator, taskId, airflowVersion, composerVersion);
  }

  /**
   * Creates a builder for GcpComposerJobFacet
   * @return a new builder for GcpComposerJobFacet
   */
  public GcpComposerJobFacetBuilder newGcpComposerJobFacetBuilder() {
    return new GcpComposerJobFacetBuilder();
  }

  /**
   * Factory method for OutputDataset
   *
   * @param namespace The namespace containing that dataset
   * @param name The unique name for that dataset within that namespace
   * @param facets The facets for this dataset
   * @param outputFacets The output facets for this dataset
   * @return OutputDataset
   */
  public OutputDataset newOutputDataset(String namespace, String name, DatasetFacets facets,
      OutputDatasetOutputFacets outputFacets) {
    return new OutputDataset(namespace, name, facets, outputFacets);
  }

  /**
   * Creates a builder for OutputDataset
   * @return a new builder for OutputDataset
   */
  public OutputDatasetBuilder newOutputDatasetBuilder() {
    return new OutputDatasetBuilder();
  }

  /**
   * Factory method for TagsDatasetFacetFields
   *
   * @param key Key that identifies the tag
   * @param value The value of the field
   * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
   * @param field Identifies the field in a dataset if a tag applies to one
   * @return TagsDatasetFacetFields
   */
  public TagsDatasetFacetFields newTagsDatasetFacetFields(String key, String value, String source,
      String field) {
    return new TagsDatasetFacetFields(key, value, source, field);
  }

  /**
   * Creates a builder for TagsDatasetFacetFields
   * @return a new builder for TagsDatasetFacetFields
   */
  public TagsDatasetFacetFieldsBuilder newTagsDatasetFacetFieldsBuilder() {
    return new TagsDatasetFacetFieldsBuilder();
  }

  /**
   * Factory method for RootJob
   *
   * @param namespace The namespace containing root job
   * @param name The unique name containing root job within that namespace
   * @return RootJob
   */
  public RootJob newRootJob(String namespace, String name) {
    return new RootJob(namespace, name);
  }

  /**
   * Creates a builder for RootJob
   * @return a new builder for RootJob
   */
  public RootJobBuilder newRootJobBuilder() {
    return new RootJobBuilder();
  }

  /**
   * Factory method for LineageEntry
   *
   * @param namespace The namespace of the target entity.
   * @param name The name of the target entity.
   * @param type The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
   * @param inputs Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
   * @param fields Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
   * @return LineageEntry
   */
  public LineageEntry newLineageEntry(String namespace, String name, String type,
      List<LineageInput> inputs, LineageEntryFields fields) {
    return new LineageEntry(namespace, name, type, inputs, fields);
  }

  /**
   * Creates a builder for LineageEntry
   * @return a new builder for LineageEntry
   */
  public LineageEntryBuilder newLineageEntryBuilder() {
    return new LineageEntryBuilder();
  }

  /**
   * Factory method for LineageDatasetTransformation
   *
   * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
   * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
   * @param description A string representation of the transformation applied.
   * @param masking Whether the transformation masks the data (e.g., hashing PII).
   * @return LineageDatasetTransformation
   */
  public LineageDatasetTransformation newLineageDatasetTransformation(String type, String subtype,
      String description, Boolean masking) {
    return new LineageDatasetTransformation(type, subtype, description, masking);
  }

  /**
   * Creates a builder for LineageDatasetTransformation
   * @return a new builder for LineageDatasetTransformation
   */
  public LineageDatasetTransformationBuilder newLineageDatasetTransformationBuilder() {
    return new LineageDatasetTransformationBuilder();
  }

  /**
   * Factory method for DataQualityMetricsDatasetFacet
   *
   * @param rowCount The number of rows evaluated
   * @param bytes The size in bytes
   * @param fileCount The number of files evaluated
   * @param lastUpdated The last time the dataset was changed
   * @param columnMetrics The property key is the column name
   * @return DataQualityMetricsDatasetFacet
   */
  public DataQualityMetricsDatasetFacet newDataQualityMetricsDatasetFacet(Long rowCount, Long bytes,
      Long fileCount, ZonedDateTime lastUpdated,
      DataQualityMetricsDatasetFacetColumnMetrics columnMetrics) {
    return new DataQualityMetricsDatasetFacet(this.producer, rowCount, bytes, fileCount, lastUpdated, columnMetrics);
  }

  /**
   * Creates a builder for DataQualityMetricsDatasetFacet
   * @return a new builder for DataQualityMetricsDatasetFacet
   */
  public DataQualityMetricsDatasetFacetBuilder newDataQualityMetricsDatasetFacetBuilder() {
    return new DataQualityMetricsDatasetFacetBuilder();
  }

  /**
   * Factory method for DocumentationDatasetFacet
   *
   * @param description The description of the dataset.
   * @param contentType MIME type of the description field content.
   * @return DocumentationDatasetFacet
   */
  public DocumentationDatasetFacet newDocumentationDatasetFacet(String description,
      String contentType) {
    return new DocumentationDatasetFacet(this.producer, description, contentType);
  }

  /**
   * Creates a builder for DocumentationDatasetFacet
   * @return a new builder for DocumentationDatasetFacet
   */
  public DocumentationDatasetFacetBuilder newDocumentationDatasetFacetBuilder() {
    return new DocumentationDatasetFacetBuilder();
  }

  /**
   * Factory method for SourceCodeLocationJobFacet
   *
   * @param type the source control system
   * @param url the full http URL to locate the file
   * @param repoUrl the URL to the repository
   * @param path the path in the repo containing the source files
   * @param version the current version deployed (not a branch name, the actual unique version)
   * @param tag optional tag name
   * @param branch optional branch name
   * @param pullRequestNumber optional pull request or merge request number associated with a CI run, populated from CI platform environment variables (e.g. GITHUB_REF, CI_MERGE_REQUEST_IID)
   * @return SourceCodeLocationJobFacet
   */
  public SourceCodeLocationJobFacet newSourceCodeLocationJobFacet(String type, URI url,
      String repoUrl, String path, String version, String tag, String branch,
      String pullRequestNumber) {
    return new SourceCodeLocationJobFacet(this.producer, type, url, repoUrl, path, version, tag, branch, pullRequestNumber);
  }

  /**
   * Creates a builder for SourceCodeLocationJobFacet
   * @return a new builder for SourceCodeLocationJobFacet
   */
  public SourceCodeLocationJobFacetBuilder newSourceCodeLocationJobFacetBuilder() {
    return new SourceCodeLocationJobFacetBuilder();
  }

  /**
   * Factory method for IcebergScanReportInputDatasetFacet
   *
   * @param snapshotId Snapshot ID of the iceberg table
   * @param filterDescription Filter used to scan the iceberg table
   * @param schemaId Schema ID of the iceberg table
   * @param projectedFieldNames List of field names that are projected from the iceberg table
   * @param scanMetrics the scanMetrics
   * @param metadata the metadata
   * @return IcebergScanReportInputDatasetFacet
   */
  public IcebergScanReportInputDatasetFacet newIcebergScanReportInputDatasetFacet(Double snapshotId,
      String filterDescription, Double schemaId, List<String> projectedFieldNames,
      IcebergScanReportInputDatasetFacetScanMetrics scanMetrics,
      IcebergScanReportInputDatasetFacetMetadata metadata) {
    return new IcebergScanReportInputDatasetFacet(this.producer, snapshotId, filterDescription, schemaId, projectedFieldNames, scanMetrics, metadata);
  }

  /**
   * Creates a builder for IcebergScanReportInputDatasetFacet
   * @return a new builder for IcebergScanReportInputDatasetFacet
   */
  public IcebergScanReportInputDatasetFacetBuilder newIcebergScanReportInputDatasetFacetBuilder() {
    return new IcebergScanReportInputDatasetFacetBuilder();
  }

  /**
   * Factory method for DatasetTypeDatasetFacet
   *
   * @param datasetType Dataset type, for example: TABLE|VIEW|FILE|TOPIC|STREAM|MODEL|JOB_OUTPUT.
   * @param subType Optional sub-type within the dataset type (e.g., MATERIALIZED, EXTERNAL, TEMPORARY).
   * @return DatasetTypeDatasetFacet
   */
  public DatasetTypeDatasetFacet newDatasetTypeDatasetFacet(String datasetType, String subType) {
    return new DatasetTypeDatasetFacet(this.producer, datasetType, subType);
  }

  /**
   * Creates a builder for DatasetTypeDatasetFacet
   * @return a new builder for DatasetTypeDatasetFacet
   */
  public DatasetTypeDatasetFacetBuilder newDatasetTypeDatasetFacetBuilder() {
    return new DatasetTypeDatasetFacetBuilder();
  }

  /**
   * Factory method for PartitionSubsetConditionPartitions
   *
   * @param identifier Optionally provided identifier of the partition specified
   * @param dimensions the dimensions
   * @return PartitionSubsetConditionPartitions
   */
  public PartitionSubsetConditionPartitions newPartitionSubsetConditionPartitions(String identifier,
      PartitionSubsetConditionPartitionsDimensions dimensions) {
    return new PartitionSubsetConditionPartitions(identifier, dimensions);
  }

  /**
   * Creates a builder for PartitionSubsetConditionPartitions
   * @return a new builder for PartitionSubsetConditionPartitions
   */
  public PartitionSubsetConditionPartitionsBuilder newPartitionSubsetConditionPartitionsBuilder() {
    return new PartitionSubsetConditionPartitionsBuilder();
  }

  /**
   * Factory method for Job
   *
   * @param namespace The namespace containing that job
   * @param name The unique name for that job within that namespace
   * @param facets The job facets.
   * @return Job
   */
  public Job newJob(String namespace, String name, JobFacets facets) {
    return new Job(namespace, name, facets);
  }

  /**
   * Creates a builder for Job
   * @return a new builder for Job
   */
  public JobBuilder newJobBuilder() {
    return new JobBuilder();
  }

  /**
   * Factory method for DatasetVersionDatasetFacet
   *
   * @param datasetVersion The version of the dataset.
   * @return DatasetVersionDatasetFacet
   */
  public DatasetVersionDatasetFacet newDatasetVersionDatasetFacet(String datasetVersion) {
    return new DatasetVersionDatasetFacet(this.producer, datasetVersion);
  }

  /**
   * Creates a builder for DatasetVersionDatasetFacet
   * @return a new builder for DatasetVersionDatasetFacet
   */
  public DatasetVersionDatasetFacetBuilder newDatasetVersionDatasetFacetBuilder() {
    return new DatasetVersionDatasetFacetBuilder();
  }

  /**
   * Factory method for SymlinksDatasetFacetIdentifiers
   *
   * @param namespace The dataset namespace
   * @param name The dataset name
   * @param type Identifier type
   * @return SymlinksDatasetFacetIdentifiers
   */
  public SymlinksDatasetFacetIdentifiers newSymlinksDatasetFacetIdentifiers(String namespace,
      String name, String type) {
    return new SymlinksDatasetFacetIdentifiers(namespace, name, type);
  }

  /**
   * Creates a builder for SymlinksDatasetFacetIdentifiers
   * @return a new builder for SymlinksDatasetFacetIdentifiers
   */
  public SymlinksDatasetFacetIdentifiersBuilder newSymlinksDatasetFacetIdentifiersBuilder() {
    return new SymlinksDatasetFacetIdentifiersBuilder();
  }

  /**
   * Factory method for LineageJobEntry
   *
   * @param namespace The namespace of the target entity.
   * @param name The name of the target entity.
   * @param type The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
   * @param inputs Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
   * @param fields Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
   * @return LineageJobEntry
   */
  public LineageJobEntry newLineageJobEntry(String namespace, String name, String type,
      List<LineageJobInput> inputs, LineageJobEntryFields fields) {
    return new LineageJobEntry(namespace, name, type, inputs, fields);
  }

  /**
   * Creates a builder for LineageJobEntry
   * @return a new builder for LineageJobEntry
   */
  public LineageJobEntryBuilder newLineageJobEntryBuilder() {
    return new LineageJobEntryBuilder();
  }

  /**
   * Factory method for TagsJobFacetFields
   *
   * @param key Key that identifies the tag
   * @param value The value of the field
   * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
   * @return TagsJobFacetFields
   */
  public TagsJobFacetFields newTagsJobFacetFields(String key, String value, String source) {
    return new TagsJobFacetFields(key, value, source);
  }

  /**
   * Creates a builder for TagsJobFacetFields
   * @return a new builder for TagsJobFacetFields
   */
  public TagsJobFacetFieldsBuilder newTagsJobFacetFieldsBuilder() {
    return new TagsJobFacetFieldsBuilder();
  }

  /**
   * @return OutputDatasetFacet
   */
  public OutputDatasetFacet newOutputDatasetFacet() {
    return new DefaultOutputDatasetFacet(this.producer);
  }

  /**
   * Factory method for StaticDataset
   *
   * @param namespace The namespace containing that dataset
   * @param name The unique name for that dataset within that namespace
   * @param facets The facets for this dataset
   * @return StaticDataset
   */
  public StaticDataset newStaticDataset(String namespace, String name, DatasetFacets facets) {
    return new StaticDataset(namespace, name, facets);
  }

  /**
   * Creates a builder for StaticDataset
   * @return a new builder for StaticDataset
   */
  public StaticDatasetBuilder newStaticDatasetBuilder() {
    return new StaticDatasetBuilder();
  }

  /**
   * Factory method for LineageJobEntryFields
   *
   * @return LineageJobEntryFields
   */
  public LineageJobEntryFields newLineageJobEntryFields() {
    return new LineageJobEntryFields();
  }

  /**
   * Creates a builder for LineageJobEntryFields
   * @return a new builder for LineageJobEntryFields
   */
  public LineageJobEntryFieldsBuilder newLineageJobEntryFieldsBuilder() {
    return new LineageJobEntryFieldsBuilder();
  }

  /**
   * Factory method for PartitionSubsetConditionPartitionsDimensions
   *
   * @return PartitionSubsetConditionPartitionsDimensions
   */
  public PartitionSubsetConditionPartitionsDimensions newPartitionSubsetConditionPartitionsDimensions(
      ) {
    return new PartitionSubsetConditionPartitionsDimensions();
  }

  /**
   * Creates a builder for PartitionSubsetConditionPartitionsDimensions
   * @return a new builder for PartitionSubsetConditionPartitionsDimensions
   */
  public PartitionSubsetConditionPartitionsDimensionsBuilder newPartitionSubsetConditionPartitionsDimensionsBuilder(
      ) {
    return new PartitionSubsetConditionPartitionsDimensionsBuilder();
  }

  /**
   * Factory method for CompareSubsetCondition
   *
   * @param left the left
   * @param right the right
   * @param comparison Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN', 'LESS_EQUAL_THAN'
   * @return CompareSubsetCondition
   */
  public CompareSubsetCondition newCompareSubsetCondition(FieldBaseCompareExpression left,
      FieldBaseCompareExpression right, String comparison) {
    return new CompareSubsetCondition(left, right, comparison);
  }

  /**
   * Creates a builder for CompareSubsetCondition
   * @return a new builder for CompareSubsetCondition
   */
  public CompareSubsetConditionBuilder newCompareSubsetConditionBuilder() {
    return new CompareSubsetConditionBuilder();
  }

  /**
   * Factory method for LineageJobFieldEntry
   *
   * @param inputs Source entities and/or fields that feed into this target field.
   * @return LineageJobFieldEntry
   */
  public LineageJobFieldEntry newLineageJobFieldEntry(List<LineageJobInput> inputs) {
    return new LineageJobFieldEntry(inputs);
  }

  /**
   * Creates a builder for LineageJobFieldEntry
   * @return a new builder for LineageJobFieldEntry
   */
  public LineageJobFieldEntryBuilder newLineageJobFieldEntryBuilder() {
    return new LineageJobFieldEntryBuilder();
  }

  /**
   * Factory method for TagsRunFacet
   *
   * @param tags The tags applied to the run facet
   * @return TagsRunFacet
   */
  public TagsRunFacet newTagsRunFacet(List<TagsRunFacetFields> tags) {
    return new TagsRunFacet(this.producer, tags);
  }

  /**
   * Creates a builder for TagsRunFacet
   * @return a new builder for TagsRunFacet
   */
  public TagsRunFacetBuilder newTagsRunFacetBuilder() {
    return new TagsRunFacetBuilder();
  }

  /**
   * Factory method for JobDependency
   *
   * @param job the job
   * @param run the run
   * @param dependency_type Used to describe whether the upstream job directly triggers the downstream job, or whether the dependency is implicit (e.g. time-based).
   * @param sequence_trigger_rule Used to describe the exact sequence condition on which the downstream job can be executed (FINISH_TO_START - downstream job can start when upstream finished; FINISH_TO_FINISH - job executions can overlap, but need to finish in specified order; START_TO_START - jobs need to start at the same time in parallel).
   * @param status_trigger_rule Used to describe if the downstream job should be run based on the status of the upstream job.
   * @return JobDependency
   */
  public JobDependency newJobDependency(JobIdentifier job, RunIdentifier run,
      String dependency_type, String sequence_trigger_rule, String status_trigger_rule) {
    return new JobDependency(job, run, dependency_type, sequence_trigger_rule, status_trigger_rule);
  }

  /**
   * Creates a builder for JobDependency
   * @return a new builder for JobDependency
   */
  public JobDependencyBuilder newJobDependencyBuilder() {
    return new JobDependencyBuilder();
  }

  /**
   * Factory method for LifecycleStateChangeDatasetFacet
   *
   * @param lifecycleStateChange The lifecycle state change.
   * @param previousIdentifier Previous name of the dataset in case of renaming it.
   * @return LifecycleStateChangeDatasetFacet
   */
  public LifecycleStateChangeDatasetFacet newLifecycleStateChangeDatasetFacet(
      LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange,
      LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier) {
    return new LifecycleStateChangeDatasetFacet(this.producer, lifecycleStateChange, previousIdentifier);
  }

  /**
   * Creates a builder for LifecycleStateChangeDatasetFacet
   * @return a new builder for LifecycleStateChangeDatasetFacet
   */
  public LifecycleStateChangeDatasetFacetBuilder newLifecycleStateChangeDatasetFacetBuilder() {
    return new LifecycleStateChangeDatasetFacetBuilder();
  }

  /**
   * Factory method for RunEvent
   *
   * @param eventTime the time the event occurred at
   * @param eventType the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete
   * @param run the run
   * @param job the job
   * @param inputs The set of **input** datasets.
   * @param outputs The set of **output** datasets.
   * @return RunEvent
   */
  public RunEvent newRunEvent(ZonedDateTime eventTime, RunEvent.EventType eventType, Run run,
      Job job, List<InputDataset> inputs, List<OutputDataset> outputs) {
    return new RunEvent(eventTime, this.producer, eventType, run, job, inputs, outputs);
  }

  /**
   * Creates a builder for RunEvent
   * @return a new builder for RunEvent
   */
  public RunEventBuilder newRunEventBuilder() {
    return new RunEventBuilder();
  }

  /**
   * Factory method for TagsDatasetFacet
   *
   * @param tags The tags applied to the dataset facet
   * @return TagsDatasetFacet
   */
  public TagsDatasetFacet newTagsDatasetFacet(List<TagsDatasetFacetFields> tags) {
    return new TagsDatasetFacet(this.producer, tags);
  }

  /**
   * Creates a builder for TagsDatasetFacet
   * @return a new builder for TagsDatasetFacet
   */
  public TagsDatasetFacetBuilder newTagsDatasetFacetBuilder() {
    return new TagsDatasetFacetBuilder();
  }

  /**
   * Factory method for CatalogDatasetFacetCatalogProperties
   *
   * @return CatalogDatasetFacetCatalogProperties
   */
  public CatalogDatasetFacetCatalogProperties newCatalogDatasetFacetCatalogProperties() {
    return new CatalogDatasetFacetCatalogProperties();
  }

  /**
   * Creates a builder for CatalogDatasetFacetCatalogProperties
   * @return a new builder for CatalogDatasetFacetCatalogProperties
   */
  public CatalogDatasetFacetCatalogPropertiesBuilder newCatalogDatasetFacetCatalogPropertiesBuilder(
      ) {
    return new CatalogDatasetFacetCatalogPropertiesBuilder();
  }

  /**
   * Factory method for ExecutionParameter
   *
   * @param key Unique identifier of the property.
   * @param name Human-readable name of the property.
   * @param description Human-readable description of the property.
   * @param value Value of the property.
   * @return ExecutionParameter
   */
  public ExecutionParameter newExecutionParameter(String key, String name, String description,
      String value) {
    return new ExecutionParameter(key, name, description, value);
  }

  /**
   * Creates a builder for ExecutionParameter
   * @return a new builder for ExecutionParameter
   */
  public ExecutionParameterBuilder newExecutionParameterBuilder() {
    return new ExecutionParameterBuilder();
  }

  /**
   * Factory method for TestRunFacet
   *
   * @param tests List of test executions and their results.
   * @return TestRunFacet
   */
  public TestRunFacet newTestRunFacet(List<TestExecution> tests) {
    return new TestRunFacet(this.producer, tests);
  }

  /**
   * Creates a builder for TestRunFacet
   * @return a new builder for TestRunFacet
   */
  public TestRunFacetBuilder newTestRunFacetBuilder() {
    return new TestRunFacetBuilder();
  }

  /**
   * Factory method for RootRun
   *
   * @param runId The globally unique ID of the root run associated with the root job.
   * @return RootRun
   */
  public RootRun newRootRun(UUID runId) {
    return new RootRun(runId);
  }

  /**
   * Creates a builder for RootRun
   * @return a new builder for RootRun
   */
  public RootRunBuilder newRootRunBuilder() {
    return new RootRunBuilder();
  }

  /**
   * @return RunFacet
   */
  public RunFacet newRunFacet() {
    return new DefaultRunFacet(this.producer);
  }

  /**
   * Factory method for HierarchyDatasetFacetLevel
   *
   * @param type Dataset hierarchy level type
   * @param name Dataset hierarchy level name
   * @return HierarchyDatasetFacetLevel
   */
  public HierarchyDatasetFacetLevel newHierarchyDatasetFacetLevel(String type, String name) {
    return new HierarchyDatasetFacetLevel(type, name);
  }

  /**
   * Creates a builder for HierarchyDatasetFacetLevel
   * @return a new builder for HierarchyDatasetFacetLevel
   */
  public HierarchyDatasetFacetLevelBuilder newHierarchyDatasetFacetLevelBuilder() {
    return new HierarchyDatasetFacetLevelBuilder();
  }

  /**
   * Factory method for OwnershipJobFacetOwners
   *
   * @param name the identifier of the owner of the Job. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
   * @param type The type of ownership (optional)
   * @return OwnershipJobFacetOwners
   */
  public OwnershipJobFacetOwners newOwnershipJobFacetOwners(String name, String type) {
    return new OwnershipJobFacetOwners(name, type);
  }

  /**
   * Creates a builder for OwnershipJobFacetOwners
   * @return a new builder for OwnershipJobFacetOwners
   */
  public OwnershipJobFacetOwnersBuilder newOwnershipJobFacetOwnersBuilder() {
    return new OwnershipJobFacetOwnersBuilder();
  }

  /**
   * Factory method for GcpLineageJobFacet
   *
   * @param displayName The name of the job to be used on UI
   * @param origin the origin
   * @return GcpLineageJobFacet
   */
  public GcpLineageJobFacet newGcpLineageJobFacet(String displayName,
      GcpLineageJobFacetOrigin origin) {
    return new GcpLineageJobFacet(this.producer, displayName, origin);
  }

  /**
   * Creates a builder for GcpLineageJobFacet
   * @return a new builder for GcpLineageJobFacet
   */
  public GcpLineageJobFacetBuilder newGcpLineageJobFacetBuilder() {
    return new GcpLineageJobFacetBuilder();
  }

  /**
   * Factory method for JobEvent
   *
   * @param eventTime the time the event occurred at
   * @param job the job
   * @param inputs The set of **input** datasets.
   * @param outputs The set of **output** datasets.
   * @return JobEvent
   */
  public JobEvent newJobEvent(ZonedDateTime eventTime, Job job, List<InputDataset> inputs,
      List<OutputDataset> outputs) {
    return new JobEvent(eventTime, this.producer, job, inputs, outputs);
  }

  /**
   * Creates a builder for JobEvent
   * @return a new builder for JobEvent
   */
  public JobEventBuilder newJobEventBuilder() {
    return new JobEventBuilder();
  }

  /**
   * Factory method for ExtractionErrorRunFacet
   *
   * @param totalTasks The number of distinguishable tasks in a run that were processed by OpenLineage, whether successfully or not. Those could be, for example, distinct SQL statements.
   * @param failedTasks The number of distinguishable tasks in a run that were processed not successfully by OpenLineage. Those could be, for example, distinct SQL statements.
   * @param errors the errors
   * @return ExtractionErrorRunFacet
   */
  public ExtractionErrorRunFacet newExtractionErrorRunFacet(Long totalTasks, Long failedTasks,
      List<ExtractionErrorRunFacetErrors> errors) {
    return new ExtractionErrorRunFacet(this.producer, totalTasks, failedTasks, errors);
  }

  /**
   * Creates a builder for ExtractionErrorRunFacet
   * @return a new builder for ExtractionErrorRunFacet
   */
  public ExtractionErrorRunFacetBuilder newExtractionErrorRunFacetBuilder() {
    return new ExtractionErrorRunFacetBuilder();
  }

  /**
   * Factory method for OwnershipDatasetFacetOwners
   *
   * @param name the identifier of the owner of the Dataset. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
   * @param type The type of ownership (optional)
   * @return OwnershipDatasetFacetOwners
   */
  public OwnershipDatasetFacetOwners newOwnershipDatasetFacetOwners(String name, String type) {
    return new OwnershipDatasetFacetOwners(name, type);
  }

  /**
   * Creates a builder for OwnershipDatasetFacetOwners
   * @return a new builder for OwnershipDatasetFacetOwners
   */
  public OwnershipDatasetFacetOwnersBuilder newOwnershipDatasetFacetOwnersBuilder() {
    return new OwnershipDatasetFacetOwnersBuilder();
  }

  /**
   * Factory method for LineageJobFacet
   *
   * @param lineage Lineage entries describing data flow declared for this job definition. Each entry identifies a target entity and the sources that feed into it.
   * @return LineageJobFacet
   */
  public LineageJobFacet newLineageJobFacet(List<LineageJobEntry> lineage) {
    return new LineageJobFacet(this.producer, lineage);
  }

  /**
   * Creates a builder for LineageJobFacet
   * @return a new builder for LineageJobFacet
   */
  public LineageJobFacetBuilder newLineageJobFacetBuilder() {
    return new LineageJobFacetBuilder();
  }

  /**
   * Factory method for DataQualityMetricsInputDatasetFacetColumnMetrics
   *
   * @return DataQualityMetricsInputDatasetFacetColumnMetrics
   */
  public DataQualityMetricsInputDatasetFacetColumnMetrics newDataQualityMetricsInputDatasetFacetColumnMetrics(
      ) {
    return new DataQualityMetricsInputDatasetFacetColumnMetrics();
  }

  /**
   * Creates a builder for DataQualityMetricsInputDatasetFacetColumnMetrics
   * @return a new builder for DataQualityMetricsInputDatasetFacetColumnMetrics
   */
  public DataQualityMetricsInputDatasetFacetColumnMetricsBuilder newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder(
      ) {
    return new DataQualityMetricsInputDatasetFacetColumnMetricsBuilder();
  }

  /**
   * Factory method for LineageEntryFields
   *
   * @return LineageEntryFields
   */
  public LineageEntryFields newLineageEntryFields() {
    return new LineageEntryFields();
  }

  /**
   * Creates a builder for LineageEntryFields
   * @return a new builder for LineageEntryFields
   */
  public LineageEntryFieldsBuilder newLineageEntryFieldsBuilder() {
    return new LineageEntryFieldsBuilder();
  }

  /**
   * Factory method for LineageDatasetFieldEntry
   *
   * @param inputs Source entities and/or fields that feed into this target field.
   * @return LineageDatasetFieldEntry
   */
  public LineageDatasetFieldEntry newLineageDatasetFieldEntry(List<LineageDatasetInput> inputs) {
    return new LineageDatasetFieldEntry(inputs);
  }

  /**
   * Creates a builder for LineageDatasetFieldEntry
   * @return a new builder for LineageDatasetFieldEntry
   */
  public LineageDatasetFieldEntryBuilder newLineageDatasetFieldEntryBuilder() {
    return new LineageDatasetFieldEntryBuilder();
  }

  /**
   * Factory method for InputField
   *
   * @param namespace The input dataset namespace
   * @param name The input dataset name
   * @param field The input field
   * @param transformations the transformations
   * @return InputField
   */
  public InputField newInputField(String namespace, String name, String field,
      List<InputFieldTransformations> transformations) {
    return new InputField(namespace, name, field, transformations);
  }

  /**
   * Creates a builder for InputField
   * @return a new builder for InputField
   */
  public InputFieldBuilder newInputFieldBuilder() {
    return new InputFieldBuilder();
  }

  /**
   * Factory method for InputStatisticsInputDatasetFacet
   *
   * @param rowCount The number of rows read
   * @param size The size in bytes read
   * @param fileCount The number of files read
   * @return InputStatisticsInputDatasetFacet
   */
  public InputStatisticsInputDatasetFacet newInputStatisticsInputDatasetFacet(Long rowCount,
      Long size, Long fileCount) {
    return new InputStatisticsInputDatasetFacet(this.producer, rowCount, size, fileCount);
  }

  /**
   * Creates a builder for InputStatisticsInputDatasetFacet
   * @return a new builder for InputStatisticsInputDatasetFacet
   */
  public InputStatisticsInputDatasetFacetBuilder newInputStatisticsInputDatasetFacetBuilder() {
    return new InputStatisticsInputDatasetFacetBuilder();
  }

  /**
   * Factory method for JobFacets
   *
   * @param jobType the jobType
   * @param sourceCode the sourceCode
   * @param gcp_lineage the gcp_lineage
   * @param sql the sql
   * @param lineage the lineage
   * @param gcp_composer_job the gcp_composer_job
   * @param ownership the ownership
   * @param sourceCodeLocation the sourceCodeLocation
   * @param tags the tags
   * @param documentation the documentation
   * @return JobFacets
   */
  public JobFacets newJobFacets(JobTypeJobFacet jobType, SourceCodeJobFacet sourceCode,
      GcpLineageJobFacet gcp_lineage, SQLJobFacet sql, LineageJobFacet lineage,
      GcpComposerJobFacet gcp_composer_job, OwnershipJobFacet ownership,
      SourceCodeLocationJobFacet sourceCodeLocation, TagsJobFacet tags,
      DocumentationJobFacet documentation) {
    return new JobFacets(jobType, sourceCode, gcp_lineage, sql, lineage, gcp_composer_job, ownership, sourceCodeLocation, tags, documentation);
  }

  /**
   * Creates a builder for JobFacets
   * @return a new builder for JobFacets
   */
  public JobFacetsBuilder newJobFacetsBuilder() {
    return new JobFacetsBuilder();
  }

  /**
   * Factory method for CatalogDatasetFacet
   *
   * @param framework The storage framework for which the catalog is configured
   * @param type Type of the catalog.
   * @param name Name of the catalog, as configured in the source system.
   * @param metadataUri URI or connection string to the catalog, if applicable.
   * @param warehouseUri URI or connection string to the physical location of the data that the catalog describes.
   * @param source Source system where the catalog is configured.
   * @param catalogProperties Additional catalog properties
   * @return CatalogDatasetFacet
   */
  public CatalogDatasetFacet newCatalogDatasetFacet(String framework, String type, String name,
      String metadataUri, String warehouseUri, String source,
      CatalogDatasetFacetCatalogProperties catalogProperties) {
    return new CatalogDatasetFacet(this.producer, framework, type, name, metadataUri, warehouseUri, source, catalogProperties);
  }

  /**
   * Creates a builder for CatalogDatasetFacet
   * @return a new builder for CatalogDatasetFacet
   */
  public CatalogDatasetFacetBuilder newCatalogDatasetFacetBuilder() {
    return new CatalogDatasetFacetBuilder();
  }

  /**
   * @return DatasetFacet
   */
  public DatasetFacet newDatasetFacet() {
    return new DefaultDatasetFacet(this.producer, null);
  }

  /**
   * @return a deleted DatasetFacet
   */
  public DatasetFacet newDeletedDatasetFacet() {
    return new DefaultDatasetFacet(this.producer, true);
  }

  /**
   * Factory method for OwnershipDatasetFacet
   *
   * @param owners The owners of the dataset.
   * @return OwnershipDatasetFacet
   */
  public OwnershipDatasetFacet newOwnershipDatasetFacet(List<OwnershipDatasetFacetOwners> owners) {
    return new OwnershipDatasetFacet(this.producer, owners);
  }

  /**
   * Creates a builder for OwnershipDatasetFacet
   * @return a new builder for OwnershipDatasetFacet
   */
  public OwnershipDatasetFacetBuilder newOwnershipDatasetFacetBuilder() {
    return new OwnershipDatasetFacetBuilder();
  }

  /**
   * Factory method for EnvironmentVariablesRunFacet
   *
   * @param environmentVariables The environment variables for the run.
   * @return EnvironmentVariablesRunFacet
   */
  public EnvironmentVariablesRunFacet newEnvironmentVariablesRunFacet(
      List<EnvironmentVariable> environmentVariables) {
    return new EnvironmentVariablesRunFacet(this.producer, environmentVariables);
  }

  /**
   * Creates a builder for EnvironmentVariablesRunFacet
   * @return a new builder for EnvironmentVariablesRunFacet
   */
  public EnvironmentVariablesRunFacetBuilder newEnvironmentVariablesRunFacetBuilder() {
    return new EnvironmentVariablesRunFacetBuilder();
  }

  /**
   * Factory method for JobDependenciesRunFacet
   *
   * @param upstream Job runs that must complete before the current run can start.
   * @param downstream Job runs that will start after completion of the current run.
   * @param trigger_rule Specifies the condition under which this job will run based on the status of upstream jobs.
   * @return JobDependenciesRunFacet
   */
  public JobDependenciesRunFacet newJobDependenciesRunFacet(List<JobDependency> upstream,
      List<JobDependency> downstream, String trigger_rule) {
    return new JobDependenciesRunFacet(this.producer, upstream, downstream, trigger_rule);
  }

  /**
   * Creates a builder for JobDependenciesRunFacet
   * @return a new builder for JobDependenciesRunFacet
   */
  public JobDependenciesRunFacetBuilder newJobDependenciesRunFacetBuilder() {
    return new JobDependenciesRunFacetBuilder();
  }

  /**
   * Factory method for DatasetFacets
   *
   * @param hierarchy the hierarchy
   * @param dataSource the dataSource
   * @param version the version
   * @param datasetType the datasetType
   * @param storage the storage
   * @param columnLineage the columnLineage
   * @param lifecycleStateChange the lifecycleStateChange
   * @param dataQualityMetrics the dataQualityMetrics
   * @param lineage the lineage
   * @param tags the tags
   * @param documentation the documentation
   * @param schema the schema
   * @param ownership the ownership
   * @param catalog the catalog
   * @param symlinks the symlinks
   * @return DatasetFacets
   */
  public DatasetFacets newDatasetFacets(HierarchyDatasetFacet hierarchy,
      DatasourceDatasetFacet dataSource, DatasetVersionDatasetFacet version,
      DatasetTypeDatasetFacet datasetType, StorageDatasetFacet storage,
      ColumnLineageDatasetFacet columnLineage,
      LifecycleStateChangeDatasetFacet lifecycleStateChange,
      DataQualityMetricsDatasetFacet dataQualityMetrics, LineageDatasetFacet lineage,
      TagsDatasetFacet tags, DocumentationDatasetFacet documentation, SchemaDatasetFacet schema,
      OwnershipDatasetFacet ownership, CatalogDatasetFacet catalog, SymlinksDatasetFacet symlinks) {
    return new DatasetFacets(hierarchy, dataSource, version, datasetType, storage, columnLineage, lifecycleStateChange, dataQualityMetrics, lineage, tags, documentation, schema, ownership, catalog, symlinks);
  }

  /**
   * Creates a builder for DatasetFacets
   * @return a new builder for DatasetFacets
   */
  public DatasetFacetsBuilder newDatasetFacetsBuilder() {
    return new DatasetFacetsBuilder();
  }

  /**
   * Factory method for GcpComposerRunFacet
   *
   * @param dagRunId The id of the DAG run
   * @return GcpComposerRunFacet
   */
  public GcpComposerRunFacet newGcpComposerRunFacet(String dagRunId) {
    return new GcpComposerRunFacet(this.producer, dagRunId);
  }

  /**
   * Creates a builder for GcpComposerRunFacet
   * @return a new builder for GcpComposerRunFacet
   */
  public GcpComposerRunFacetBuilder newGcpComposerRunFacetBuilder() {
    return new GcpComposerRunFacetBuilder();
  }

  /**
   * Factory method for TestExecutionParams
   *
   * @return TestExecutionParams
   */
  public TestExecutionParams newTestExecutionParams() {
    return new TestExecutionParams();
  }

  /**
   * Creates a builder for TestExecutionParams
   * @return a new builder for TestExecutionParams
   */
  public TestExecutionParamsBuilder newTestExecutionParamsBuilder() {
    return new TestExecutionParamsBuilder();
  }

  /**
   * Factory method for OwnershipJobFacet
   *
   * @param owners The owners of the job.
   * @return OwnershipJobFacet
   */
  public OwnershipJobFacet newOwnershipJobFacet(List<OwnershipJobFacetOwners> owners) {
    return new OwnershipJobFacet(this.producer, owners);
  }

  /**
   * Creates a builder for OwnershipJobFacet
   * @return a new builder for OwnershipJobFacet
   */
  public OwnershipJobFacetBuilder newOwnershipJobFacetBuilder() {
    return new OwnershipJobFacetBuilder();
  }

  /**
   * Factory method for RunIdentifier
   *
   * @param runId The globally unique ID of the run.
   * @return RunIdentifier
   */
  public RunIdentifier newRunIdentifier(UUID runId) {
    return new RunIdentifier(runId);
  }

  /**
   * Creates a builder for RunIdentifier
   * @return a new builder for RunIdentifier
   */
  public RunIdentifierBuilder newRunIdentifierBuilder() {
    return new RunIdentifierBuilder();
  }

  /**
   * Factory method for FieldBaseCompareExpression
   *
   * @param field the field
   * @return FieldBaseCompareExpression
   */
  public FieldBaseCompareExpression newFieldBaseCompareExpression(String field) {
    return new FieldBaseCompareExpression(field);
  }

  /**
   * Creates a builder for FieldBaseCompareExpression
   * @return a new builder for FieldBaseCompareExpression
   */
  public FieldBaseCompareExpressionBuilder newFieldBaseCompareExpressionBuilder() {
    return new FieldBaseCompareExpressionBuilder();
  }

  /**
   * Factory method for IcebergCommitReportOutputDatasetFacetMetadata
   *
   * @return IcebergCommitReportOutputDatasetFacetMetadata
   */
  public IcebergCommitReportOutputDatasetFacetMetadata newIcebergCommitReportOutputDatasetFacetMetadata(
      ) {
    return new IcebergCommitReportOutputDatasetFacetMetadata();
  }

  /**
   * Creates a builder for IcebergCommitReportOutputDatasetFacetMetadata
   * @return a new builder for IcebergCommitReportOutputDatasetFacetMetadata
   */
  public IcebergCommitReportOutputDatasetFacetMetadataBuilder newIcebergCommitReportOutputDatasetFacetMetadataBuilder(
      ) {
    return new IcebergCommitReportOutputDatasetFacetMetadataBuilder();
  }

  /**
   * Factory method for ParentRunFacetJob
   *
   * @param namespace The namespace containing that job
   * @param name The unique name for that job within that namespace
   * @return ParentRunFacetJob
   */
  public ParentRunFacetJob newParentRunFacetJob(String namespace, String name) {
    return new ParentRunFacetJob(namespace, name);
  }

  /**
   * Creates a builder for ParentRunFacetJob
   * @return a new builder for ParentRunFacetJob
   */
  public ParentRunFacetJobBuilder newParentRunFacetJobBuilder() {
    return new ParentRunFacetJobBuilder();
  }

  /**
   * Factory method for GcpLineageJobFacetOrigin
   *
   * @param sourceType Type of the source. Possible values can be found in GCP documentation (https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#SourceType) 
   * @param name If the sourceType isn't CUSTOM, the value of this field should be a GCP resource name of the system, which reports lineage. The project and location parts of the resource name must match the project and location of the lineage resource being created. More details in GCP documentation https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#origin
   * @return GcpLineageJobFacetOrigin
   */
  public GcpLineageJobFacetOrigin newGcpLineageJobFacetOrigin(String sourceType, String name) {
    return new GcpLineageJobFacetOrigin(sourceType, name);
  }

  /**
   * Creates a builder for GcpLineageJobFacetOrigin
   * @return a new builder for GcpLineageJobFacetOrigin
   */
  public GcpLineageJobFacetOriginBuilder newGcpLineageJobFacetOriginBuilder() {
    return new GcpLineageJobFacetOriginBuilder();
  }

  /**
   * Factory method for ErrorMessageRunFacet
   *
   * @param message A human-readable string representing error message generated by observed system
   * @param programmingLanguage Programming language the observed system uses.
   * @param stackTrace A language-specific stack trace generated by observed system
   * @return ErrorMessageRunFacet
   */
  public ErrorMessageRunFacet newErrorMessageRunFacet(String message, String programmingLanguage,
      String stackTrace) {
    return new ErrorMessageRunFacet(this.producer, message, programmingLanguage, stackTrace);
  }

  /**
   * Creates a builder for ErrorMessageRunFacet
   * @return a new builder for ErrorMessageRunFacet
   */
  public ErrorMessageRunFacetBuilder newErrorMessageRunFacetBuilder() {
    return new ErrorMessageRunFacetBuilder();
  }

  /**
   * Factory method for IcebergCommitReportOutputDatasetFacetCommitMetrics
   *
   * @param totalDuration Duration of the commit in MILLISECONDS
   * @param attempts Number of attempts made to commit the iceberg table
   * @param addedDataFiles Number of data files that are added during the commit
   * @param removedDataFiles Number of data files that are removed during the commit
   * @param totalDataFiles Total number of data files that are present in the iceberg table
   * @param addedDeleteFiles Number of delete files that are added during the commit
   * @param addedEqualityDeleteFiles Number of added equality delete files
   * @param addedPositionalDeleteFiles Number of added positional delete files
   * @param addedDVs Number of added DVs
   * @param removedDeleteFiles Number of delete files that are removed during the commit
   * @param removedEqualityDeleteFiles Number of removed equality delete files
   * @param removedPositionalDeleteFiles Number of removed positional delete files
   * @param removedDVs Number of removed DVs
   * @param totalDeleteFiles Total number of temporary delete files that are present in the iceberg table
   * @param addedRecords Number of records that are added during the commit
   * @param removedRecords Number of records that are removed during the commit
   * @param totalRecords Number of records that are present in the iceberg table
   * @param addedFilesSizeInBytes Number of files size in bytes that are added during the commit
   * @param removedFilesSizeInBytes Number of files size in bytes that are removed during the commit
   * @param totalFilesSizeInBytes Number of files size in bytes in the iceberg table
   * @param addedPositionalDeletes Number of positional deletes that are added during the commit
   * @param removedPositionalDeletes Number of positional deletes that are removed during the commit
   * @param totalPositionalDeletes Number of positional deletes that are present in the iceberg table
   * @param addedEqualityDeletes Number of equality deletes that are added during the commit
   * @param removedEqualityDeletes Number of equality deletes that are removed during the commit
   * @param totalEqualityDeletes Number of equality deletes that are present in the iceberg table
   * @return IcebergCommitReportOutputDatasetFacetCommitMetrics
   */
  public IcebergCommitReportOutputDatasetFacetCommitMetrics newIcebergCommitReportOutputDatasetFacetCommitMetrics(
      Double totalDuration, Double attempts, Double addedDataFiles, Double removedDataFiles,
      Double totalDataFiles, Double addedDeleteFiles, Double addedEqualityDeleteFiles,
      Double addedPositionalDeleteFiles, Double addedDVs, Double removedDeleteFiles,
      Double removedEqualityDeleteFiles, Double removedPositionalDeleteFiles, Double removedDVs,
      Double totalDeleteFiles, Double addedRecords, Double removedRecords, Double totalRecords,
      Double addedFilesSizeInBytes, Double removedFilesSizeInBytes, Double totalFilesSizeInBytes,
      Double addedPositionalDeletes, Double removedPositionalDeletes, Double totalPositionalDeletes,
      Double addedEqualityDeletes, Double removedEqualityDeletes, Double totalEqualityDeletes) {
    return new IcebergCommitReportOutputDatasetFacetCommitMetrics(totalDuration, attempts, addedDataFiles, removedDataFiles, totalDataFiles, addedDeleteFiles, addedEqualityDeleteFiles, addedPositionalDeleteFiles, addedDVs, removedDeleteFiles, removedEqualityDeleteFiles, removedPositionalDeleteFiles, removedDVs, totalDeleteFiles, addedRecords, removedRecords, totalRecords, addedFilesSizeInBytes, removedFilesSizeInBytes, totalFilesSizeInBytes, addedPositionalDeletes, removedPositionalDeletes, totalPositionalDeletes, addedEqualityDeletes, removedEqualityDeletes, totalEqualityDeletes);
  }

  /**
   * Creates a builder for IcebergCommitReportOutputDatasetFacetCommitMetrics
   * @return a new builder for IcebergCommitReportOutputDatasetFacetCommitMetrics
   */
  public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder newIcebergCommitReportOutputDatasetFacetCommitMetricsBuilder(
      ) {
    return new IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder();
  }

  /**
   * @return JobFacet
   */
  public JobFacet newJobFacet() {
    return new DefaultJobFacet(this.producer, null);
  }

  /**
   * @return a deleted JobFacet
   */
  public JobFacet newDeletedJobFacet() {
    return new DefaultJobFacet(this.producer, true);
  }

  /**
   * Factory method for RunFacets
   *
   * @param externalQuery the externalQuery
   * @param gcp_dataproc the gcp_dataproc
   * @param extractionError the extractionError
   * @param parent the parent
   * @param nominalTime the nominalTime
   * @param tags the tags
   * @param errorMessage the errorMessage
   * @param environmentVariables the environmentVariables
   * @param lineage the lineage
   * @param test the test
   * @param gcp_composer_run the gcp_composer_run
   * @param executionParameters the executionParameters
   * @param jobDependencies the jobDependencies
   * @param processing_engine the processing_engine
   * @return RunFacets
   */
  public RunFacets newRunFacets(ExternalQueryRunFacet externalQuery,
      GcpDataprocRunFacet gcp_dataproc, ExtractionErrorRunFacet extractionError,
      ParentRunFacet parent, NominalTimeRunFacet nominalTime, TagsRunFacet tags,
      ErrorMessageRunFacet errorMessage, EnvironmentVariablesRunFacet environmentVariables,
      LineageRunFacet lineage, TestRunFacet test, GcpComposerRunFacet gcp_composer_run,
      ExecutionParametersRunFacet executionParameters, JobDependenciesRunFacet jobDependencies,
      ProcessingEngineRunFacet processing_engine) {
    return new RunFacets(externalQuery, gcp_dataproc, extractionError, parent, nominalTime, tags, errorMessage, environmentVariables, lineage, test, gcp_composer_run, executionParameters, jobDependencies, processing_engine);
  }

  /**
   * Creates a builder for RunFacets
   * @return a new builder for RunFacets
   */
  public RunFacetsBuilder newRunFacetsBuilder() {
    return new RunFacetsBuilder();
  }

  /**
   * Factory method for SchemaDatasetFacet
   *
   * @param fields The fields of the data source.
   * @return SchemaDatasetFacet
   */
  public SchemaDatasetFacet newSchemaDatasetFacet(List<SchemaDatasetFacetFields> fields) {
    return new SchemaDatasetFacet(this.producer, fields);
  }

  /**
   * Creates a builder for SchemaDatasetFacet
   * @return a new builder for SchemaDatasetFacet
   */
  public SchemaDatasetFacetBuilder newSchemaDatasetFacetBuilder() {
    return new SchemaDatasetFacetBuilder();
  }

  /**
   * Factory method for LineageJobInput
   *
   * @param namespace The namespace of the source entity.
   * @param name The name of the source entity.
   * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
   * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
   * @param transformations Transformations applied to the source data.
   * @return LineageJobInput
   */
  public LineageJobInput newLineageJobInput(String namespace, String name, String type,
      String field, List<LineageJobTransformation> transformations) {
    return new LineageJobInput(namespace, name, type, field, transformations);
  }

  /**
   * Creates a builder for LineageJobInput
   * @return a new builder for LineageJobInput
   */
  public LineageJobInputBuilder newLineageJobInputBuilder() {
    return new LineageJobInputBuilder();
  }

  /**
   * Factory method for LocationSubsetCondition
   *
   * @param locations the locations
   * @return LocationSubsetCondition
   */
  public LocationSubsetCondition newLocationSubsetCondition(List<String> locations) {
    return new LocationSubsetCondition(locations);
  }

  /**
   * Creates a builder for LocationSubsetCondition
   * @return a new builder for LocationSubsetCondition
   */
  public LocationSubsetConditionBuilder newLocationSubsetConditionBuilder() {
    return new LocationSubsetConditionBuilder();
  }

  /**
   * Factory method for SourceCodeJobFacet
   *
   * @param language Language in which source code of this job was written.
   * @param sourceCode Source code of this job.
   * @return SourceCodeJobFacet
   */
  public SourceCodeJobFacet newSourceCodeJobFacet(String language, String sourceCode) {
    return new SourceCodeJobFacet(this.producer, language, sourceCode);
  }

  /**
   * Creates a builder for SourceCodeJobFacet
   * @return a new builder for SourceCodeJobFacet
   */
  public SourceCodeJobFacetBuilder newSourceCodeJobFacetBuilder() {
    return new SourceCodeJobFacetBuilder();
  }

  /**
   * Factory method for IcebergScanReportInputDatasetFacetMetadata
   *
   * @return IcebergScanReportInputDatasetFacetMetadata
   */
  public IcebergScanReportInputDatasetFacetMetadata newIcebergScanReportInputDatasetFacetMetadata(
      ) {
    return new IcebergScanReportInputDatasetFacetMetadata();
  }

  /**
   * Creates a builder for IcebergScanReportInputDatasetFacetMetadata
   * @return a new builder for IcebergScanReportInputDatasetFacetMetadata
   */
  public IcebergScanReportInputDatasetFacetMetadataBuilder newIcebergScanReportInputDatasetFacetMetadataBuilder(
      ) {
    return new IcebergScanReportInputDatasetFacetMetadataBuilder();
  }

  /**
   * Factory method for ColumnLineageDatasetFacet
   *
   * @param fields Column level lineage that maps output fields into input fields used to evaluate them.
   * @param dataset Column level lineage that affects the whole dataset. This includes filtering, sorting, grouping (aggregates), joining, window functions, etc.
   * @return ColumnLineageDatasetFacet
   */
  public ColumnLineageDatasetFacet newColumnLineageDatasetFacet(
      ColumnLineageDatasetFacetFields fields, List<InputField> dataset) {
    return new ColumnLineageDatasetFacet(this.producer, fields, dataset);
  }

  /**
   * Creates a builder for ColumnLineageDatasetFacet
   * @return a new builder for ColumnLineageDatasetFacet
   */
  public ColumnLineageDatasetFacetBuilder newColumnLineageDatasetFacetBuilder() {
    return new ColumnLineageDatasetFacetBuilder();
  }

  /**
   * Factory method for DataQualityAssertionsDatasetFacet
   *
   * @param assertions the assertions
   * @return DataQualityAssertionsDatasetFacet
   */
  public DataQualityAssertionsDatasetFacet newDataQualityAssertionsDatasetFacet(
      List<DataQualityAssertionsDatasetFacetAssertions> assertions) {
    return new DataQualityAssertionsDatasetFacet(this.producer, assertions);
  }

  /**
   * Creates a builder for DataQualityAssertionsDatasetFacet
   * @return a new builder for DataQualityAssertionsDatasetFacet
   */
  public DataQualityAssertionsDatasetFacetBuilder newDataQualityAssertionsDatasetFacetBuilder() {
    return new DataQualityAssertionsDatasetFacetBuilder();
  }

  /**
   * Factory method for DatasetEvent
   *
   * @param eventTime the time the event occurred at
   * @param dataset the dataset
   * @return DatasetEvent
   */
  public DatasetEvent newDatasetEvent(ZonedDateTime eventTime, StaticDataset dataset) {
    return new DatasetEvent(eventTime, this.producer, dataset);
  }

  /**
   * Creates a builder for DatasetEvent
   * @return a new builder for DatasetEvent
   */
  public DatasetEventBuilder newDatasetEventBuilder() {
    return new DatasetEventBuilder();
  }

  /**
   * Factory method for TagsJobFacet
   *
   * @param tags The tags applied to the job facet
   * @return TagsJobFacet
   */
  public TagsJobFacet newTagsJobFacet(List<TagsJobFacetFields> tags) {
    return new TagsJobFacet(this.producer, tags);
  }

  /**
   * Creates a builder for TagsJobFacet
   * @return a new builder for TagsJobFacet
   */
  public TagsJobFacetBuilder newTagsJobFacetBuilder() {
    return new TagsJobFacetBuilder();
  }

  /**
   * Factory method for LineageFieldEntry
   *
   * @param inputs Source entities and/or fields that feed into this target field.
   * @return LineageFieldEntry
   */
  public LineageFieldEntry newLineageFieldEntry(List<LineageInput> inputs) {
    return new LineageFieldEntry(inputs);
  }

  /**
   * Creates a builder for LineageFieldEntry
   * @return a new builder for LineageFieldEntry
   */
  public LineageFieldEntryBuilder newLineageFieldEntryBuilder() {
    return new LineageFieldEntryBuilder();
  }

  /**
   * Factory method for ProcessingEngineRunFacet
   *
   * @param version Processing engine version. Might be Airflow or Spark version.
   * @param name Processing engine name, e.g. Airflow or Spark
   * @param openlineageAdapterVersion OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration package version
   * @return ProcessingEngineRunFacet
   */
  public ProcessingEngineRunFacet newProcessingEngineRunFacet(String version, String name,
      String openlineageAdapterVersion) {
    return new ProcessingEngineRunFacet(this.producer, version, name, openlineageAdapterVersion);
  }

  /**
   * Creates a builder for ProcessingEngineRunFacet
   * @return a new builder for ProcessingEngineRunFacet
   */
  public ProcessingEngineRunFacetBuilder newProcessingEngineRunFacetBuilder() {
    return new ProcessingEngineRunFacetBuilder();
  }

  /**
   * Factory method for LifecycleStateChangeDatasetFacetPreviousIdentifier
   *
   * @param name the name
   * @param namespace the namespace
   * @return LifecycleStateChangeDatasetFacetPreviousIdentifier
   */
  public LifecycleStateChangeDatasetFacetPreviousIdentifier newLifecycleStateChangeDatasetFacetPreviousIdentifier(
      String name, String namespace) {
    return new LifecycleStateChangeDatasetFacetPreviousIdentifier(name, namespace);
  }

  /**
   * Creates a builder for LifecycleStateChangeDatasetFacetPreviousIdentifier
   * @return a new builder for LifecycleStateChangeDatasetFacetPreviousIdentifier
   */
  public LifecycleStateChangeDatasetFacetPreviousIdentifierBuilder newLifecycleStateChangeDatasetFacetPreviousIdentifierBuilder(
      ) {
    return new LifecycleStateChangeDatasetFacetPreviousIdentifierBuilder();
  }

  /**
   * Factory method for InputFieldTransformations
   *
   * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT
   * @param subtype The subtype of the transformation
   * @param description a string representation of the transformation applied
   * @param masking is transformation masking the data or not
   * @return InputFieldTransformations
   */
  public InputFieldTransformations newInputFieldTransformations(String type, String subtype,
      String description, Boolean masking) {
    return new InputFieldTransformations(type, subtype, description, masking);
  }

  /**
   * Creates a builder for InputFieldTransformations
   * @return a new builder for InputFieldTransformations
   */
  public InputFieldTransformationsBuilder newInputFieldTransformationsBuilder() {
    return new InputFieldTransformationsBuilder();
  }

  /**
   * Factory method for LineageJobTransformation
   *
   * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
   * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
   * @param description A string representation of the transformation applied.
   * @param masking Whether the transformation masks the data (e.g., hashing PII).
   * @return LineageJobTransformation
   */
  public LineageJobTransformation newLineageJobTransformation(String type, String subtype,
      String description, Boolean masking) {
    return new LineageJobTransformation(type, subtype, description, masking);
  }

  /**
   * Creates a builder for LineageJobTransformation
   * @return a new builder for LineageJobTransformation
   */
  public LineageJobTransformationBuilder newLineageJobTransformationBuilder() {
    return new LineageJobTransformationBuilder();
  }

  /**
   * Factory method for PartitionSubsetCondition
   *
   * @param partitions the partitions
   * @return PartitionSubsetCondition
   */
  public PartitionSubsetCondition newPartitionSubsetCondition(
      List<PartitionSubsetConditionPartitions> partitions) {
    return new PartitionSubsetCondition(partitions);
  }

  /**
   * Creates a builder for PartitionSubsetCondition
   * @return a new builder for PartitionSubsetCondition
   */
  public PartitionSubsetConditionBuilder newPartitionSubsetConditionBuilder() {
    return new PartitionSubsetConditionBuilder();
  }

  /**
   * Factory method for EnvironmentVariable
   *
   * @param name The name of the environment variable.
   * @param value The value of the environment variable.
   * @return EnvironmentVariable
   */
  public EnvironmentVariable newEnvironmentVariable(String name, String value) {
    return new EnvironmentVariable(name, value);
  }

  /**
   * Creates a builder for EnvironmentVariable
   * @return a new builder for EnvironmentVariable
   */
  public EnvironmentVariableBuilder newEnvironmentVariableBuilder() {
    return new EnvironmentVariableBuilder();
  }

  /**
   * Factory method for IcebergScanReportInputDatasetFacetScanMetrics
   *
   * @param totalPlanningDuration Duration of the scan in MILLISECONDS
   * @param resultDataFiles List of data files that are read during the scan
   * @param resultDeleteFiles List of delete files that are read during the scan
   * @param totalDataManifests Total number of manifests that are scanned during the scan
   * @param totalDeleteManifests Total number of delete manifests that are scanned during the scan
   * @param scannedDataManifests Number of data manifests that are scanned during the scan
   * @param skippedDataManifests Number of data manifests that are skipped during the scan
   * @param totalFileSizeInBytes Total file size in bytes that are read during the scan
   * @param totalDeleteFileSizeInBytes Total delete file size in bytes that are read during the scan
   * @param skippedDataFiles Number of data files that are skipped during the scan
   * @param skippedDeleteFiles Number of delete files that are skipped during the scan
   * @param scannedDeleteManifests Number of delete manifests that are scanned during the scan
   * @param skippedDeleteManifests Number of delete manifests that are skipped during the scan
   * @param indexedDeleteFiles Number of delete files that are indexed during the scan
   * @param equalityDeleteFiles Number of delete files that are equality indexed during the scan
   * @param positionalDeleteFiles Number of delete files that are positional indexed during the scan
   * @return IcebergScanReportInputDatasetFacetScanMetrics
   */
  public IcebergScanReportInputDatasetFacetScanMetrics newIcebergScanReportInputDatasetFacetScanMetrics(
      Double totalPlanningDuration, Double resultDataFiles, Double resultDeleteFiles,
      Double totalDataManifests, Double totalDeleteManifests, Double scannedDataManifests,
      Double skippedDataManifests, Double totalFileSizeInBytes, Double totalDeleteFileSizeInBytes,
      Double skippedDataFiles, Double skippedDeleteFiles, Double scannedDeleteManifests,
      Double skippedDeleteManifests, Double indexedDeleteFiles, Double equalityDeleteFiles,
      Double positionalDeleteFiles) {
    return new IcebergScanReportInputDatasetFacetScanMetrics(totalPlanningDuration, resultDataFiles, resultDeleteFiles, totalDataManifests, totalDeleteManifests, scannedDataManifests, skippedDataManifests, totalFileSizeInBytes, totalDeleteFileSizeInBytes, skippedDataFiles, skippedDeleteFiles, scannedDeleteManifests, skippedDeleteManifests, indexedDeleteFiles, equalityDeleteFiles, positionalDeleteFiles);
  }

  /**
   * Creates a builder for IcebergScanReportInputDatasetFacetScanMetrics
   * @return a new builder for IcebergScanReportInputDatasetFacetScanMetrics
   */
  public IcebergScanReportInputDatasetFacetScanMetricsBuilder newIcebergScanReportInputDatasetFacetScanMetricsBuilder(
      ) {
    return new IcebergScanReportInputDatasetFacetScanMetricsBuilder();
  }

  /**
   * Factory method for TestExecution
   *
   * @param name Name identifying the test.
   * @param status Whether the test found issues: 'pass' (no issues found), 'fail' (issues found), 'skip' (not executed). Independent of severity — a test can fail without blocking the pipeline when severity is 'warn'.
   * @param severity The configured consequence of a test failure: 'error' (blocks pipeline execution) or 'warn' (produces a warning only, does not block). A test with severity 'warn' and status 'fail' means issues were found but execution continued.
   * @param type Classification of the test, e.g. 'not_null', 'unique', 'row_count', 'freshness', 'custom_sql'.
   * @param description Human-readable description of what the test checks.
   * @param expected The expected value or threshold for the test, serialized as a string.
   * @param actual The actual value observed during the test, serialized as a string.
   * @param content The test body, e.g. a SQL query or expression.
   * @param contentType The format of the content field, allowing consumers to interpret or filter test content. Common values include 'sql', 'json', 'expression'.
   * @param params Arbitrary key-value pairs for check-specific inputs.
   * @return TestExecution
   */
  public TestExecution newTestExecution(String name, String status, String severity, String type,
      String description, String expected, String actual, String content, String contentType,
      TestExecutionParams params) {
    return new TestExecution(name, status, severity, type, description, expected, actual, content, contentType, params);
  }

  /**
   * Creates a builder for TestExecution
   * @return a new builder for TestExecution
   */
  public TestExecutionBuilder newTestExecutionBuilder() {
    return new TestExecutionBuilder();
  }

  /**
   * Factory method for TagsRunFacetFields
   *
   * @param key Key that identifies the tag
   * @param value The value of the field
   * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
   * @return TagsRunFacetFields
   */
  public TagsRunFacetFields newTagsRunFacetFields(String key, String value, String source) {
    return new TagsRunFacetFields(key, value, source);
  }

  /**
   * Creates a builder for TagsRunFacetFields
   * @return a new builder for TagsRunFacetFields
   */
  public TagsRunFacetFieldsBuilder newTagsRunFacetFieldsBuilder() {
    return new TagsRunFacetFieldsBuilder();
  }

  /**
   * Factory method for DataQualityMetricsDatasetFacetColumnMetricsAdditional
   *
   * @param nullCount The number of null values in this column for the rows evaluated
   * @param distinctCount The number of distinct values in this column for the rows evaluated
   * @param sum The total sum of values in this column for the rows evaluated
   * @param count The number of values in this column
   * @param min the min
   * @param max the max
   * @param quantiles The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
   * @return DataQualityMetricsDatasetFacetColumnMetricsAdditional
   */
  public DataQualityMetricsDatasetFacetColumnMetricsAdditional newDataQualityMetricsDatasetFacetColumnMetricsAdditional(
      Long nullCount, Long distinctCount, Double sum, Double count, Double min, Double max,
      DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles quantiles) {
    return new DataQualityMetricsDatasetFacetColumnMetricsAdditional(nullCount, distinctCount, sum, count, min, max, quantiles);
  }

  /**
   * Creates a builder for DataQualityMetricsDatasetFacetColumnMetricsAdditional
   * @return a new builder for DataQualityMetricsDatasetFacetColumnMetricsAdditional
   */
  public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder newDataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder(
      ) {
    return new DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder();
  }

  /**
   * Factory method for DataQualityAssertionsDatasetFacetAssertions
   *
   * @param assertion Type of expectation test that dataset is subjected to
   * @param success the success
   * @param column Column that expectation is testing. It should match the name provided in SchemaDatasetFacet. If column field is empty, then expectation refers to whole dataset.
   * @param severity The configured severity level of the assertion. Common values are 'error' (test failure blocks pipeline) or 'warn' (test failure produces warning only).
   * @return DataQualityAssertionsDatasetFacetAssertions
   */
  public DataQualityAssertionsDatasetFacetAssertions newDataQualityAssertionsDatasetFacetAssertions(
      String assertion, Boolean success, String column, String severity) {
    return new DataQualityAssertionsDatasetFacetAssertions(assertion, success, column, severity);
  }

  /**
   * Creates a builder for DataQualityAssertionsDatasetFacetAssertions
   * @return a new builder for DataQualityAssertionsDatasetFacetAssertions
   */
  public DataQualityAssertionsDatasetFacetAssertionsBuilder newDataQualityAssertionsDatasetFacetAssertionsBuilder(
      ) {
    return new DataQualityAssertionsDatasetFacetAssertionsBuilder();
  }

  /**
   * Factory method for ParentRunFacetRun
   *
   * @param runId The globally unique ID of the run associated with the job.
   * @return ParentRunFacetRun
   */
  public ParentRunFacetRun newParentRunFacetRun(UUID runId) {
    return new ParentRunFacetRun(runId);
  }

  /**
   * Creates a builder for ParentRunFacetRun
   * @return a new builder for ParentRunFacetRun
   */
  public ParentRunFacetRunBuilder newParentRunFacetRunBuilder() {
    return new ParentRunFacetRunBuilder();
  }

  /**
   * Factory method for GcpDataprocRunFacet
   *
   * @param appId Application ID set by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
   * @param appName App name which may be provided by the user, or some default is used by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
   * @param batchId Populated only for Dataproc serverless batches. The resource id of the batch.
   * @param batchUuid Populated only for Dataproc serverless batches. A UUID generated by the service when it creates the batch.
   * @param clusterName Populated only for Dataproc GCE workloads. The cluster name is unique within a GCP project.
   * @param clusterUuid Populated only for Dataproc GCE workloads. A UUID generated by the service at the time of cluster creation.
   * @param jobId Populated only for Dataproc GCE workloads. If not specified by the user, the job ID will be provided by the service.
   * @param jobUuid Populated only for Dataproc GCE workloads. A UUID that uniquely identifies a job within the project over time.
   * @param projectId The GCP project ID that the resource belongs to.
   * @param queryNodeName The name of the query node in the executed Spark Plan. Often used to describe the command being executed.
   * @param jobType Identifies whether the process is a job (on a Dataproc cluster), a batch or a session.
   * @param sessionId Populated only for Dataproc serverless interactive sessions. The resource id of the session, used for URL generation.
   * @param sessionUuid Populated only for Dataproc serverless interactive sessions. A UUID generated by the service when it creates the session.
   * @return GcpDataprocRunFacet
   */
  public GcpDataprocRunFacet newGcpDataprocRunFacet(String appId, String appName, String batchId,
      String batchUuid, String clusterName, String clusterUuid, String jobId, String jobUuid,
      String projectId, String queryNodeName, String jobType, String sessionId,
      String sessionUuid) {
    return new GcpDataprocRunFacet(this.producer, appId, appName, batchId, batchUuid, clusterName, clusterUuid, jobId, jobUuid, projectId, queryNodeName, jobType, sessionId, sessionUuid);
  }

  /**
   * Creates a builder for GcpDataprocRunFacet
   * @return a new builder for GcpDataprocRunFacet
   */
  public GcpDataprocRunFacetBuilder newGcpDataprocRunFacetBuilder() {
    return new GcpDataprocRunFacetBuilder();
  }

  /**
   * Factory method for DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles
   *
   * @return DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles
   */
  public DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles newDataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles(
      ) {
    return new DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles();
  }

  /**
   * Creates a builder for DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles
   * @return a new builder for DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles
   */
  public DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantilesBuilder newDataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantilesBuilder(
      ) {
    return new DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantilesBuilder();
  }

  /**
   * Factory method for IcebergCommitReportOutputDatasetFacet
   *
   * @param snapshotId Snapshot ID of the iceberg table
   * @param sequenceNumber Sequence number of the iceberg table
   * @param operation Operation that was performed on the iceberg table
   * @param commitMetrics the commitMetrics
   * @param metadata the metadata
   * @return IcebergCommitReportOutputDatasetFacet
   */
  public IcebergCommitReportOutputDatasetFacet newIcebergCommitReportOutputDatasetFacet(
      Double snapshotId, Double sequenceNumber, String operation,
      IcebergCommitReportOutputDatasetFacetCommitMetrics commitMetrics,
      IcebergCommitReportOutputDatasetFacetMetadata metadata) {
    return new IcebergCommitReportOutputDatasetFacet(this.producer, snapshotId, sequenceNumber, operation, commitMetrics, metadata);
  }

  /**
   * Creates a builder for IcebergCommitReportOutputDatasetFacet
   * @return a new builder for IcebergCommitReportOutputDatasetFacet
   */
  public IcebergCommitReportOutputDatasetFacetBuilder newIcebergCommitReportOutputDatasetFacetBuilder(
      ) {
    return new IcebergCommitReportOutputDatasetFacetBuilder();
  }

  /**
   * Factory method for ColumnLineageDatasetFacetFields
   *
   * @return ColumnLineageDatasetFacetFields
   */
  public ColumnLineageDatasetFacetFields newColumnLineageDatasetFacetFields() {
    return new ColumnLineageDatasetFacetFields();
  }

  /**
   * Creates a builder for ColumnLineageDatasetFacetFields
   * @return a new builder for ColumnLineageDatasetFacetFields
   */
  public ColumnLineageDatasetFacetFieldsBuilder newColumnLineageDatasetFacetFieldsBuilder() {
    return new ColumnLineageDatasetFacetFieldsBuilder();
  }

  /**
   * Factory method for HierarchyDatasetFacet
   *
   * @param hierarchy Dataset hierarchy levels (e.g. DATABASE -> SCHEMA -> TABLE), from highest to lowest level. The order is important
   * @return HierarchyDatasetFacet
   */
  public HierarchyDatasetFacet newHierarchyDatasetFacet(
      List<HierarchyDatasetFacetLevel> hierarchy) {
    return new HierarchyDatasetFacet(this.producer, hierarchy);
  }

  /**
   * Creates a builder for HierarchyDatasetFacet
   * @return a new builder for HierarchyDatasetFacet
   */
  public HierarchyDatasetFacetBuilder newHierarchyDatasetFacetBuilder() {
    return new HierarchyDatasetFacetBuilder();
  }

  /**
   * Factory method for ExecutionParametersRunFacet
   *
   * @param parameters The parameters passed to the Job at runtime
   * @return ExecutionParametersRunFacet
   */
  public ExecutionParametersRunFacet newExecutionParametersRunFacet(
      List<ExecutionParameter> parameters) {
    return new ExecutionParametersRunFacet(this.producer, parameters);
  }

  /**
   * Creates a builder for ExecutionParametersRunFacet
   * @return a new builder for ExecutionParametersRunFacet
   */
  public ExecutionParametersRunFacetBuilder newExecutionParametersRunFacetBuilder() {
    return new ExecutionParametersRunFacetBuilder();
  }

  /**
   * Factory method for SymlinksDatasetFacet
   *
   * @param identifiers the identifiers
   * @return SymlinksDatasetFacet
   */
  public SymlinksDatasetFacet newSymlinksDatasetFacet(
      List<SymlinksDatasetFacetIdentifiers> identifiers) {
    return new SymlinksDatasetFacet(this.producer, identifiers);
  }

  /**
   * Creates a builder for SymlinksDatasetFacet
   * @return a new builder for SymlinksDatasetFacet
   */
  public SymlinksDatasetFacetBuilder newSymlinksDatasetFacetBuilder() {
    return new SymlinksDatasetFacetBuilder();
  }

  /**
   * Factory method for BinarySubsetCondition
   *
   * @param left the left
   * @param right the right
   * @param operator Allowed values: 'AND' or 'OR'
   * @return BinarySubsetCondition
   */
  public BinarySubsetCondition newBinarySubsetCondition(LocationSubsetCondition left,
      LocationSubsetCondition right, String operator) {
    return new BinarySubsetCondition(left, right, operator);
  }

  /**
   * Creates a builder for BinarySubsetCondition
   * @return a new builder for BinarySubsetCondition
   */
  public BinarySubsetConditionBuilder newBinarySubsetConditionBuilder() {
    return new BinarySubsetConditionBuilder();
  }

  /**
   * Factory method for StorageDatasetFacet
   *
   * @param storageLayer Storage layer provider with allowed values: iceberg, delta.
   * @param fileFormat File format with allowed values: parquet, orc, avro, json, csv, text, xml.
   * @return StorageDatasetFacet
   */
  public StorageDatasetFacet newStorageDatasetFacet(String storageLayer, String fileFormat) {
    return new StorageDatasetFacet(this.producer, storageLayer, fileFormat);
  }

  /**
   * Creates a builder for StorageDatasetFacet
   * @return a new builder for StorageDatasetFacet
   */
  public StorageDatasetFacetBuilder newStorageDatasetFacetBuilder() {
    return new StorageDatasetFacetBuilder();
  }

  /**
   * Factory method for InputDataset
   *
   * @param namespace The namespace containing that dataset
   * @param name The unique name for that dataset within that namespace
   * @param facets The facets for this dataset
   * @param inputFacets The input facets for this dataset.
   * @return InputDataset
   */
  public InputDataset newInputDataset(String namespace, String name, DatasetFacets facets,
      InputDatasetInputFacets inputFacets) {
    return new InputDataset(namespace, name, facets, inputFacets);
  }

  /**
   * Creates a builder for InputDataset
   * @return a new builder for InputDataset
   */
  public InputDatasetBuilder newInputDatasetBuilder() {
    return new InputDatasetBuilder();
  }

  /**
   * Factory method for JobIdentifier
   *
   * @param namespace The namespace containing the job
   * @param name The unique name of a job within that namespace
   * @return JobIdentifier
   */
  public JobIdentifier newJobIdentifier(String namespace, String name) {
    return new JobIdentifier(namespace, name);
  }

  /**
   * Creates a builder for JobIdentifier
   * @return a new builder for JobIdentifier
   */
  public JobIdentifierBuilder newJobIdentifierBuilder() {
    return new JobIdentifierBuilder();
  }

  /**
   * Factory method for DatasourceDatasetFacet
   *
   * @param name the name
   * @param uri the uri
   * @return DatasourceDatasetFacet
   */
  public DatasourceDatasetFacet newDatasourceDatasetFacet(String name, URI uri) {
    return new DatasourceDatasetFacet(this.producer, name, uri);
  }

  /**
   * Creates a builder for DatasourceDatasetFacet
   * @return a new builder for DatasourceDatasetFacet
   */
  public DatasourceDatasetFacetBuilder newDatasourceDatasetFacetBuilder() {
    return new DatasourceDatasetFacetBuilder();
  }

  public interface Builder<T> {
    /**
     * @return the constructed type
     */
    T build();
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
  }

  /**
   * model class for SchemaDatasetFacetFields
   */
  @JsonDeserialize(
      as = SchemaDatasetFacetFields.class
  )
  @JsonPropertyOrder({
      "name",
      "type",
      "description",
      "ordinal_position",
      "fields"
  })
  public static final class SchemaDatasetFacetFields {
    private final String name;

    private final String type;

    private final String description;

    private final Long ordinal_position;

    private final List<SchemaDatasetFacetFields> fields;

    /**
     * @param name The name of the field.
     * @param type The type of the field.
     * @param description The description of the field.
     * @param ordinal_position The ordinal position of the field in the schema (1-indexed).
     * @param fields Nested struct fields.
     */
    @JsonCreator
    private SchemaDatasetFacetFields(@JsonProperty("name") String name,
        @JsonProperty("type") String type, @JsonProperty("description") String description,
        @JsonProperty("ordinal_position") Long ordinal_position,
        @JsonProperty("fields") List<SchemaDatasetFacetFields> fields) {
      this.name = name;
      this.type = type;
      this.description = description;
      this.ordinal_position = ordinal_position;
      this.fields = fields;
    }

    /**
     * @return The name of the field.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of the field.
     */
    public String getType() {
      return type;
    }

    /**
     * @return The description of the field.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return The ordinal position of the field in the schema (1-indexed).
     */
    public Long getOrdinal_position() {
      return ordinal_position;
    }

    /**
     * @return Nested struct fields.
     */
    public List<SchemaDatasetFacetFields> getFields() {
      return fields;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SchemaDatasetFacetFields that = (SchemaDatasetFacetFields) o;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(ordinal_position, that.ordinal_position)) return false;
      if (!Objects.equals(fields, that.fields)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type, description, ordinal_position, fields);
    }
  }

  /**
   * builder class for SchemaDatasetFacetFields
   */
  public static final class SchemaDatasetFacetFieldsBuilder implements Builder<SchemaDatasetFacetFields> {
    private String name;

    private String type;

    private String description;

    private Long ordinal_position;

    private List<SchemaDatasetFacetFields> fields;

    /**
     * @param name The name of the field.
     * @return this
     */
    public SchemaDatasetFacetFieldsBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of the field.
     * @return this
     */
    public SchemaDatasetFacetFieldsBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param description The description of the field.
     * @return this
     */
    public SchemaDatasetFacetFieldsBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param ordinal_position The ordinal position of the field in the schema (1-indexed).
     * @return this
     */
    public SchemaDatasetFacetFieldsBuilder ordinal_position(Long ordinal_position) {
      this.ordinal_position = ordinal_position;
      return this;
    }

    /**
     * @param fields Nested struct fields.
     * @return this
     */
    public SchemaDatasetFacetFieldsBuilder fields(List<SchemaDatasetFacetFields> fields) {
      this.fields = fields;
      return this;
    }

    /**
     * build an instance of SchemaDatasetFacetFields from the fields set in the builder
     */
    @Override
    public SchemaDatasetFacetFields build() {
      SchemaDatasetFacetFields __result = new SchemaDatasetFacetFields(name, type, description, ordinal_position, fields);
      return __result;
    }
  }

  /**
   * model class for LineageDatasetInput
   */
  @JsonDeserialize(
      as = LineageDatasetInput.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "type",
      "field",
      "transformations"
  })
  public static final class LineageDatasetInput {
    private final String namespace;

    private final String name;

    private final String type;

    private final String field;

    private final List<LineageDatasetTransformation> transformations;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace of the source entity.
     * @param name The name of the source entity.
     * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     * @param transformations Transformations applied to the source data.
     */
    @JsonCreator
    private LineageDatasetInput(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("type") String type,
        @JsonProperty("field") String field,
        @JsonProperty("transformations") List<LineageDatasetTransformation> transformations) {
      this.namespace = namespace;
      this.name = name;
      this.type = type;
      this.field = field;
      this.transformations = transformations;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace of the source entity.
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The name of the source entity.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     */
    public String getType() {
      return type;
    }

    /**
     * @return The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     */
    public String getField() {
      return field;
    }

    /**
     * @return Transformations applied to the source data.
     */
    public List<LineageDatasetTransformation> getTransformations() {
      return transformations;
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
      LineageDatasetInput that = (LineageDatasetInput) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(field, that.field)) return false;
      if (!Objects.equals(transformations, that.transformations)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, type, field, transformations, additionalProperties);
    }
  }

  /**
   * builder class for LineageDatasetInput
   */
  public static final class LineageDatasetInputBuilder implements Builder<LineageDatasetInput> {
    private String namespace;

    private String name;

    private String type;

    private String field;

    private List<LineageDatasetTransformation> transformations;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param namespace The namespace of the source entity.
     * @return this
     */
    public LineageDatasetInputBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The name of the source entity.
     * @return this
     */
    public LineageDatasetInputBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     * @return this
     */
    public LineageDatasetInputBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     * @return this
     */
    public LineageDatasetInputBuilder field(String field) {
      this.field = field;
      return this;
    }

    /**
     * @param transformations Transformations applied to the source data.
     * @return this
     */
    public LineageDatasetInputBuilder transformations(
        List<LineageDatasetTransformation> transformations) {
      this.transformations = transformations;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageDatasetInputBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageDatasetInput from the fields set in the builder
     */
    @Override
    public LineageDatasetInput build() {
      LineageDatasetInput __result = new LineageDatasetInput(namespace, name, type, field, transformations);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ParentRunFacet
   */
  @JsonDeserialize(
      as = ParentRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "run",
      "job",
      "root"
  })
  public static final class ParentRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final ParentRunFacetRun run;

    private final ParentRunFacetJob job;

    private final ParentRunFacetRoot root;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param run the run
     * @param job the job
     * @param root the root
     */
    @JsonCreator
    private ParentRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("run") ParentRunFacetRun run, @JsonProperty("job") ParentRunFacetJob job,
        @JsonProperty("root") ParentRunFacetRoot root) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet");
      this.run = run;
      this.job = job;
      this.root = root;
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

    public ParentRunFacetRun getRun() {
      return run;
    }

    public ParentRunFacetJob getJob() {
      return job;
    }

    public ParentRunFacetRoot getRoot() {
      return root;
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
      ParentRunFacet that = (ParentRunFacet) o;
      if (!Objects.equals(run, that.run)) return false;
      if (!Objects.equals(job, that.job)) return false;
      if (!Objects.equals(root, that.root)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(run, job, root, additionalProperties);
    }
  }

  /**
   * builder class for ParentRunFacet
   */
  public final class ParentRunFacetBuilder implements Builder<ParentRunFacet> {
    private ParentRunFacetRun run;

    private ParentRunFacetJob job;

    private ParentRunFacetRoot root;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param run the run
     * @return this
     */
    public ParentRunFacetBuilder run(ParentRunFacetRun run) {
      this.run = run;
      return this;
    }

    /**
     * @param job the job
     * @return this
     */
    public ParentRunFacetBuilder job(ParentRunFacetJob job) {
      this.job = job;
      return this;
    }

    /**
     * @param root the root
     * @return this
     */
    public ParentRunFacetBuilder root(ParentRunFacetRoot root) {
      this.root = root;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ParentRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ParentRunFacet from the fields set in the builder
     */
    @Override
    public ParentRunFacet build() {
      ParentRunFacet __result = new ParentRunFacet(OpenLineage.this.producer, run, job, root);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for OutputStatisticsOutputDatasetFacet
   */
  @JsonDeserialize(
      as = OutputStatisticsOutputDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "rowCount",
      "size",
      "fileCount"
  })
  public static final class OutputStatisticsOutputDatasetFacet implements OutputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Long rowCount;

    private final Long size;

    private final Long fileCount;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param rowCount The number of rows written to the dataset
     * @param size The size in bytes written to the dataset
     * @param fileCount The number of files written to the dataset
     */
    @JsonCreator
    private OutputStatisticsOutputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("rowCount") Long rowCount, @JsonProperty("size") Long size,
        @JsonProperty("fileCount") Long fileCount) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet");
      this.rowCount = rowCount;
      this.size = size;
      this.fileCount = fileCount;
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
     * @return The number of rows written to the dataset
     */
    public Long getRowCount() {
      return rowCount;
    }

    /**
     * @return The size in bytes written to the dataset
     */
    public Long getSize() {
      return size;
    }

    /**
     * @return The number of files written to the dataset
     */
    public Long getFileCount() {
      return fileCount;
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
      OutputStatisticsOutputDatasetFacet that = (OutputStatisticsOutputDatasetFacet) o;
      if (!Objects.equals(rowCount, that.rowCount)) return false;
      if (!Objects.equals(size, that.size)) return false;
      if (!Objects.equals(fileCount, that.fileCount)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(rowCount, size, fileCount, additionalProperties);
    }
  }

  /**
   * builder class for OutputStatisticsOutputDatasetFacet
   */
  public final class OutputStatisticsOutputDatasetFacetBuilder implements Builder<OutputStatisticsOutputDatasetFacet> {
    private Long rowCount;

    private Long size;

    private Long fileCount;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param rowCount The number of rows written to the dataset
     * @return this
     */
    public OutputStatisticsOutputDatasetFacetBuilder rowCount(Long rowCount) {
      this.rowCount = rowCount;
      return this;
    }

    /**
     * @param size The size in bytes written to the dataset
     * @return this
     */
    public OutputStatisticsOutputDatasetFacetBuilder size(Long size) {
      this.size = size;
      return this;
    }

    /**
     * @param fileCount The number of files written to the dataset
     * @return this
     */
    public OutputStatisticsOutputDatasetFacetBuilder fileCount(Long fileCount) {
      this.fileCount = fileCount;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public OutputStatisticsOutputDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of OutputStatisticsOutputDatasetFacet from the fields set in the builder
     */
    @Override
    public OutputStatisticsOutputDatasetFacet build() {
      OutputStatisticsOutputDatasetFacet __result = new OutputStatisticsOutputDatasetFacet(OpenLineage.this.producer, rowCount, size, fileCount);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for InputSubsetInputDatasetFacet
   */
  @JsonDeserialize(
      as = InputSubsetInputDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "inputCondition"
  })
  public static final class InputSubsetInputDatasetFacet implements InputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final LocationSubsetCondition inputCondition;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param inputCondition the inputCondition
     */
    @JsonCreator
    private InputSubsetInputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("inputCondition") LocationSubsetCondition inputCondition) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/InputSubsetInputDatasetFacet");
      this.inputCondition = inputCondition;
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

    public LocationSubsetCondition getInputCondition() {
      return inputCondition;
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
      InputSubsetInputDatasetFacet that = (InputSubsetInputDatasetFacet) o;
      if (!Objects.equals(inputCondition, that.inputCondition)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(inputCondition, additionalProperties);
    }
  }

  /**
   * builder class for InputSubsetInputDatasetFacet
   */
  public final class InputSubsetInputDatasetFacetBuilder implements Builder<InputSubsetInputDatasetFacet> {
    private LocationSubsetCondition inputCondition;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param inputCondition the inputCondition
     * @return this
     */
    public InputSubsetInputDatasetFacetBuilder inputCondition(
        LocationSubsetCondition inputCondition) {
      this.inputCondition = inputCondition;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public InputSubsetInputDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of InputSubsetInputDatasetFacet from the fields set in the builder
     */
    @Override
    public InputSubsetInputDatasetFacet build() {
      InputSubsetInputDatasetFacet __result = new InputSubsetInputDatasetFacet(OpenLineage.this.producer, inputCondition);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ExtractionErrorRunFacetErrors
   */
  @JsonDeserialize(
      as = ExtractionErrorRunFacetErrors.class
  )
  @JsonPropertyOrder({
      "errorMessage",
      "stackTrace",
      "task",
      "taskNumber"
  })
  public static final class ExtractionErrorRunFacetErrors {
    private final String errorMessage;

    private final String stackTrace;

    private final String task;

    private final Long taskNumber;

    /**
     * @param errorMessage Text representation of extraction error message.
     * @param stackTrace Stack trace of extraction error message
     * @param task Text representation of task that failed. This can be, for example, SQL statement that parser could not interpret.
     * @param taskNumber Order of task (counted from 0).
     */
    @JsonCreator
    private ExtractionErrorRunFacetErrors(@JsonProperty("errorMessage") String errorMessage,
        @JsonProperty("stackTrace") String stackTrace, @JsonProperty("task") String task,
        @JsonProperty("taskNumber") Long taskNumber) {
      this.errorMessage = errorMessage;
      this.stackTrace = stackTrace;
      this.task = task;
      this.taskNumber = taskNumber;
    }

    /**
     * @return Text representation of extraction error message.
     */
    public String getErrorMessage() {
      return errorMessage;
    }

    /**
     * @return Stack trace of extraction error message
     */
    public String getStackTrace() {
      return stackTrace;
    }

    /**
     * @return Text representation of task that failed. This can be, for example, SQL statement that parser could not interpret.
     */
    public String getTask() {
      return task;
    }

    /**
     * @return Order of task (counted from 0).
     */
    public Long getTaskNumber() {
      return taskNumber;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ExtractionErrorRunFacetErrors that = (ExtractionErrorRunFacetErrors) o;
      if (!Objects.equals(errorMessage, that.errorMessage)) return false;
      if (!Objects.equals(stackTrace, that.stackTrace)) return false;
      if (!Objects.equals(task, that.task)) return false;
      if (!Objects.equals(taskNumber, that.taskNumber)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(errorMessage, stackTrace, task, taskNumber);
    }
  }

  /**
   * builder class for ExtractionErrorRunFacetErrors
   */
  public static final class ExtractionErrorRunFacetErrorsBuilder implements Builder<ExtractionErrorRunFacetErrors> {
    private String errorMessage;

    private String stackTrace;

    private String task;

    private Long taskNumber;

    /**
     * @param errorMessage Text representation of extraction error message.
     * @return this
     */
    public ExtractionErrorRunFacetErrorsBuilder errorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    /**
     * @param stackTrace Stack trace of extraction error message
     * @return this
     */
    public ExtractionErrorRunFacetErrorsBuilder stackTrace(String stackTrace) {
      this.stackTrace = stackTrace;
      return this;
    }

    /**
     * @param task Text representation of task that failed. This can be, for example, SQL statement that parser could not interpret.
     * @return this
     */
    public ExtractionErrorRunFacetErrorsBuilder task(String task) {
      this.task = task;
      return this;
    }

    /**
     * @param taskNumber Order of task (counted from 0).
     * @return this
     */
    public ExtractionErrorRunFacetErrorsBuilder taskNumber(Long taskNumber) {
      this.taskNumber = taskNumber;
      return this;
    }

    /**
     * build an instance of ExtractionErrorRunFacetErrors from the fields set in the builder
     */
    @Override
    public ExtractionErrorRunFacetErrors build() {
      ExtractionErrorRunFacetErrors __result = new ExtractionErrorRunFacetErrors(errorMessage, stackTrace, task, taskNumber);
      return __result;
    }
  }

  /**
   * model class for JobTypeJobFacet
   */
  @JsonDeserialize(
      as = JobTypeJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "processingType",
      "integration",
      "jobType"
  })
  public static final class JobTypeJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String processingType;

    private final String integration;

    private final String jobType;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param processingType Job processing type like: BATCH or STREAMING
     * @param integration OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK
     * @param jobType Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific field.
     */
    @JsonCreator
    private JobTypeJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("processingType") String processingType,
        @JsonProperty("integration") String integration, @JsonProperty("jobType") String jobType) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet");
      this._deleted = null;
      this.processingType = processingType;
      this.integration = integration;
      this.jobType = jobType;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Job processing type like: BATCH or STREAMING
     */
    public String getProcessingType() {
      return processingType;
    }

    /**
     * @return OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK
     */
    public String getIntegration() {
      return integration;
    }

    /**
     * @return Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific field.
     */
    public String getJobType() {
      return jobType;
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
      JobTypeJobFacet that = (JobTypeJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(processingType, that.processingType)) return false;
      if (!Objects.equals(integration, that.integration)) return false;
      if (!Objects.equals(jobType, that.jobType)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, processingType, integration, jobType, additionalProperties);
    }
  }

  /**
   * builder class for JobTypeJobFacet
   */
  public final class JobTypeJobFacetBuilder implements Builder<JobTypeJobFacet> {
    private String processingType;

    private String integration;

    private String jobType;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param processingType Job processing type like: BATCH or STREAMING
     * @return this
     */
    public JobTypeJobFacetBuilder processingType(String processingType) {
      this.processingType = processingType;
      return this;
    }

    /**
     * @param integration OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK
     * @return this
     */
    public JobTypeJobFacetBuilder integration(String integration) {
      this.integration = integration;
      return this;
    }

    /**
     * @param jobType Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific field.
     * @return this
     */
    public JobTypeJobFacetBuilder jobType(String jobType) {
      this.jobType = jobType;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public JobTypeJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of JobTypeJobFacet from the fields set in the builder
     */
    @Override
    public JobTypeJobFacet build() {
      JobTypeJobFacet __result = new JobTypeJobFacet(OpenLineage.this.producer, processingType, integration, jobType);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for Run
   */
  @JsonDeserialize(
      as = Run.class
  )
  @JsonPropertyOrder({
      "runId",
      "facets"
  })
  public static final class Run {
    private final UUID runId;

    private final RunFacets facets;

    /**
     * @param runId The globally unique ID of the run associated with the job.
     * @param facets The run facets.
     */
    @JsonCreator
    private Run(@JsonProperty("runId") UUID runId, @JsonProperty("facets") RunFacets facets) {
      this.runId = runId;
      this.facets = facets;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Run that = (Run) o;
      if (!Objects.equals(runId, that.runId)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(runId, facets);
    }
  }

  /**
   * builder class for Run
   */
  public static final class RunBuilder implements Builder<Run> {
    private UUID runId;

    private RunFacets facets;

    /**
     * @param runId The globally unique ID of the run associated with the job.
     * @return this
     */
    public RunBuilder runId(UUID runId) {
      this.runId = runId;
      return this;
    }

    /**
     * @param facets The run facets.
     * @return this
     */
    public RunBuilder facets(RunFacets facets) {
      this.facets = facets;
      return this;
    }

    /**
     * build an instance of Run from the fields set in the builder
     */
    @Override
    public Run build() {
      Run __result = new Run(runId, facets);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsDatasetFacetColumnMetrics
   */
  @JsonDeserialize(
      as = DataQualityMetricsDatasetFacetColumnMetrics.class
  )
  @JsonPropertyOrder
  public static final class DataQualityMetricsDatasetFacetColumnMetrics {
    @JsonAnySetter
    private final Map<String, DataQualityMetricsDatasetFacetColumnMetricsAdditional> additionalProperties;

    @JsonCreator
    private DataQualityMetricsDatasetFacetColumnMetrics() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, DataQualityMetricsDatasetFacetColumnMetricsAdditional> getAdditionalProperties(
        ) {
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
      DataQualityMetricsDatasetFacetColumnMetrics that = (DataQualityMetricsDatasetFacetColumnMetrics) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for DataQualityMetricsDatasetFacetColumnMetrics
   */
  public static final class DataQualityMetricsDatasetFacetColumnMetricsBuilder implements Builder<DataQualityMetricsDatasetFacetColumnMetrics> {
    private final Map<String, DataQualityMetricsDatasetFacetColumnMetricsAdditional> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsBuilder put(String key,
        DataQualityMetricsDatasetFacetColumnMetricsAdditional value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DataQualityMetricsDatasetFacetColumnMetrics from the fields set in the builder
     */
    @Override
    public DataQualityMetricsDatasetFacetColumnMetrics build() {
      DataQualityMetricsDatasetFacetColumnMetrics __result = new DataQualityMetricsDatasetFacetColumnMetrics();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ParentRunFacetRoot
   */
  @JsonDeserialize(
      as = ParentRunFacetRoot.class
  )
  @JsonPropertyOrder({
      "run",
      "job"
  })
  public static final class ParentRunFacetRoot {
    private final RootRun run;

    private final RootJob job;

    /**
     * @param run the run
     * @param job the job
     */
    @JsonCreator
    private ParentRunFacetRoot(@JsonProperty("run") RootRun run, @JsonProperty("job") RootJob job) {
      this.run = run;
      this.job = job;
    }

    public RootRun getRun() {
      return run;
    }

    public RootJob getJob() {
      return job;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ParentRunFacetRoot that = (ParentRunFacetRoot) o;
      if (!Objects.equals(run, that.run)) return false;
      if (!Objects.equals(job, that.job)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(run, job);
    }
  }

  /**
   * builder class for ParentRunFacetRoot
   */
  public static final class ParentRunFacetRootBuilder implements Builder<ParentRunFacetRoot> {
    private RootRun run;

    private RootJob job;

    /**
     * @param run the run
     * @return this
     */
    public ParentRunFacetRootBuilder run(RootRun run) {
      this.run = run;
      return this;
    }

    /**
     * @param job the job
     * @return this
     */
    public ParentRunFacetRootBuilder job(RootJob job) {
      this.job = job;
      return this;
    }

    /**
     * build an instance of ParentRunFacetRoot from the fields set in the builder
     */
    @Override
    public ParentRunFacetRoot build() {
      ParentRunFacetRoot __result = new ParentRunFacetRoot(run, job);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles
   */
  @JsonDeserialize(
      as = DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles.class
  )
  @JsonPropertyOrder
  public static final class DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles {
    @JsonAnySetter
    private final Map<String, Double> additionalProperties;

    @JsonCreator
    private DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, Double> getAdditionalProperties() {
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
      DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles that = (DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles
   */
  public static final class DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder implements Builder<DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles> {
    private final Map<String, Double> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder put(
        String key, Double value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles from the fields set in the builder
     */
    @Override
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles build() {
      DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles __result = new DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ExternalQueryRunFacet
   */
  @JsonDeserialize(
      as = ExternalQueryRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "externalQueryId",
      "source"
  })
  public static final class ExternalQueryRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final String externalQueryId;

    private final String source;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param externalQueryId Identifier for the external system
     * @param source source of the external query
     */
    @JsonCreator
    private ExternalQueryRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("externalQueryId") String externalQueryId,
        @JsonProperty("source") String source) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-2/ExternalQueryRunFacet.json#/$defs/ExternalQueryRunFacet");
      this.externalQueryId = externalQueryId;
      this.source = source;
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
     * @return Identifier for the external system
     */
    public String getExternalQueryId() {
      return externalQueryId;
    }

    /**
     * @return source of the external query
     */
    public String getSource() {
      return source;
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
      ExternalQueryRunFacet that = (ExternalQueryRunFacet) o;
      if (!Objects.equals(externalQueryId, that.externalQueryId)) return false;
      if (!Objects.equals(source, that.source)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(externalQueryId, source, additionalProperties);
    }
  }

  /**
   * builder class for ExternalQueryRunFacet
   */
  public final class ExternalQueryRunFacetBuilder implements Builder<ExternalQueryRunFacet> {
    private String externalQueryId;

    private String source;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param externalQueryId Identifier for the external system
     * @return this
     */
    public ExternalQueryRunFacetBuilder externalQueryId(String externalQueryId) {
      this.externalQueryId = externalQueryId;
      return this;
    }

    /**
     * @param source source of the external query
     * @return this
     */
    public ExternalQueryRunFacetBuilder source(String source) {
      this.source = source;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ExternalQueryRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ExternalQueryRunFacet from the fields set in the builder
     */
    @Override
    public ExternalQueryRunFacet build() {
      ExternalQueryRunFacet __result = new ExternalQueryRunFacet(OpenLineage.this.producer, externalQueryId, source);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for OutputDatasetOutputFacets
   */
  @JsonDeserialize(
      as = OutputDatasetOutputFacets.class
  )
  @JsonPropertyOrder({
      "outputStatistics",
      "icebergCommitReport"
  })
  public static final class OutputDatasetOutputFacets {
    private final OutputStatisticsOutputDatasetFacet outputStatistics;

    private final IcebergCommitReportOutputDatasetFacet icebergCommitReport;

    @JsonAnySetter
    private final Map<String, OutputDatasetFacet> additionalProperties;

    /**
     * @param outputStatistics the outputStatistics
     * @param icebergCommitReport the icebergCommitReport
     */
    @JsonCreator
    private OutputDatasetOutputFacets(
        @JsonProperty("outputStatistics") OutputStatisticsOutputDatasetFacet outputStatistics,
        @JsonProperty("icebergCommitReport") IcebergCommitReportOutputDatasetFacet icebergCommitReport) {
      this.outputStatistics = outputStatistics;
      this.icebergCommitReport = icebergCommitReport;
      this.additionalProperties = new LinkedHashMap<>();
    }

    public OutputStatisticsOutputDatasetFacet getOutputStatistics() {
      return outputStatistics;
    }

    public IcebergCommitReportOutputDatasetFacet getIcebergCommitReport() {
      return icebergCommitReport;
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
      if (!Objects.equals(outputStatistics, that.outputStatistics)) return false;
      if (!Objects.equals(icebergCommitReport, that.icebergCommitReport)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(outputStatistics, icebergCommitReport, additionalProperties);
    }
  }

  /**
   * builder class for OutputDatasetOutputFacets
   */
  public static final class OutputDatasetOutputFacetsBuilder implements Builder<OutputDatasetOutputFacets> {
    private OutputStatisticsOutputDatasetFacet outputStatistics;

    private IcebergCommitReportOutputDatasetFacet icebergCommitReport;

    private final Map<String, OutputDatasetFacet> additionalProperties = new LinkedHashMap<>();

    /**
     * @param outputStatistics the outputStatistics
     * @return this
     */
    public OutputDatasetOutputFacetsBuilder outputStatistics(
        OutputStatisticsOutputDatasetFacet outputStatistics) {
      this.outputStatistics = outputStatistics;
      return this;
    }

    /**
     * @param icebergCommitReport the icebergCommitReport
     * @return this
     */
    public OutputDatasetOutputFacetsBuilder icebergCommitReport(
        IcebergCommitReportOutputDatasetFacet icebergCommitReport) {
      this.icebergCommitReport = icebergCommitReport;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public OutputDatasetOutputFacetsBuilder put(String key, OutputDatasetFacet value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of OutputDatasetOutputFacets from the fields set in the builder
     */
    @Override
    public OutputDatasetOutputFacets build() {
      OutputDatasetOutputFacets __result = new OutputDatasetOutputFacets(outputStatistics, icebergCommitReport);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for NominalTimeRunFacet
   */
  @JsonDeserialize(
      as = NominalTimeRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "nominalStartTime",
      "nominalEndTime"
  })
  public static final class NominalTimeRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final ZonedDateTime nominalStartTime;

    private final ZonedDateTime nominalEndTime;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param nominalStartTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time
     * @param nominalEndTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run)
     */
    @JsonCreator
    private NominalTimeRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("nominalStartTime") ZonedDateTime nominalStartTime,
        @JsonProperty("nominalEndTime") ZonedDateTime nominalEndTime) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json#/$defs/NominalTimeRunFacet");
      this.nominalStartTime = nominalStartTime;
      this.nominalEndTime = nominalEndTime;
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
     * @return An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time
     */
    public ZonedDateTime getNominalStartTime() {
      return nominalStartTime;
    }

    /**
     * @return An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run)
     */
    public ZonedDateTime getNominalEndTime() {
      return nominalEndTime;
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
      NominalTimeRunFacet that = (NominalTimeRunFacet) o;
      if (!Objects.equals(nominalStartTime, that.nominalStartTime)) return false;
      if (!Objects.equals(nominalEndTime, that.nominalEndTime)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(nominalStartTime, nominalEndTime, additionalProperties);
    }
  }

  /**
   * builder class for NominalTimeRunFacet
   */
  public final class NominalTimeRunFacetBuilder implements Builder<NominalTimeRunFacet> {
    private ZonedDateTime nominalStartTime;

    private ZonedDateTime nominalEndTime;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param nominalStartTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time (included) of the run. AKA the schedule time
     * @return this
     */
    public NominalTimeRunFacetBuilder nominalStartTime(ZonedDateTime nominalStartTime) {
      this.nominalStartTime = nominalStartTime;
      return this;
    }

    /**
     * @param nominalEndTime An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time (excluded) of the run. (Should be the nominal start time of the next run)
     * @return this
     */
    public NominalTimeRunFacetBuilder nominalEndTime(ZonedDateTime nominalEndTime) {
      this.nominalEndTime = nominalEndTime;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public NominalTimeRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of NominalTimeRunFacet from the fields set in the builder
     */
    @Override
    public NominalTimeRunFacet build() {
      NominalTimeRunFacet __result = new NominalTimeRunFacet(OpenLineage.this.producer, nominalStartTime, nominalEndTime);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LiteralCompareExpression
   */
  @JsonDeserialize(
      as = LiteralCompareExpression.class
  )
  @JsonPropertyOrder({
      "type",
      "value"
  })
  public static final class LiteralCompareExpression {
    private final String type;

    private final String value;

    /**
     * @param value the value
     */
    @JsonCreator
    private LiteralCompareExpression(@JsonProperty("value") String value) {
      this.type = "literal";
      this.value = value;
    }

    public String getType() {
      return type;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LiteralCompareExpression that = (LiteralCompareExpression) o;
      if (!Objects.equals(value, that.value)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }

  /**
   * builder class for LiteralCompareExpression
   */
  public static final class LiteralCompareExpressionBuilder implements Builder<LiteralCompareExpression> {
    private String value;

    /**
     * @param value the value
     * @return this
     */
    public LiteralCompareExpressionBuilder value(String value) {
      this.value = value;
      return this;
    }

    /**
     * build an instance of LiteralCompareExpression from the fields set in the builder
     */
    @Override
    public LiteralCompareExpression build() {
      LiteralCompareExpression __result = new LiteralCompareExpression(value);
      return __result;
    }
  }

  /**
   * model class for OutputSubsetOutputDatasetFacet
   */
  @JsonDeserialize(
      as = OutputSubsetOutputDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "outputCondition"
  })
  public static final class OutputSubsetOutputDatasetFacet implements OutputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final LocationSubsetCondition outputCondition;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param outputCondition the outputCondition
     */
    @JsonCreator
    private OutputSubsetOutputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("outputCondition") LocationSubsetCondition outputCondition) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/OutputSubsetOutputDatasetFacet");
      this.outputCondition = outputCondition;
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

    public LocationSubsetCondition getOutputCondition() {
      return outputCondition;
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
      OutputSubsetOutputDatasetFacet that = (OutputSubsetOutputDatasetFacet) o;
      if (!Objects.equals(outputCondition, that.outputCondition)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(outputCondition, additionalProperties);
    }
  }

  /**
   * builder class for OutputSubsetOutputDatasetFacet
   */
  public final class OutputSubsetOutputDatasetFacetBuilder implements Builder<OutputSubsetOutputDatasetFacet> {
    private LocationSubsetCondition outputCondition;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param outputCondition the outputCondition
     * @return this
     */
    public OutputSubsetOutputDatasetFacetBuilder outputCondition(
        LocationSubsetCondition outputCondition) {
      this.outputCondition = outputCondition;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public OutputSubsetOutputDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of OutputSubsetOutputDatasetFacet from the fields set in the builder
     */
    @Override
    public OutputSubsetOutputDatasetFacet build() {
      OutputSubsetOutputDatasetFacet __result = new OutputSubsetOutputDatasetFacet(OpenLineage.this.producer, outputCondition);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
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

  public static class DefaultInputDatasetFacet implements InputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @JsonCreator
    public DefaultInputDatasetFacet(@JsonProperty("_producer") URI _producer) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/InputDatasetFacet");
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
  }

  /**
   * Interface for InputDatasetFacet
   */
  @JsonDeserialize(
      as = DefaultInputDatasetFacet.class
  )
  public interface InputDatasetFacet {
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
   * model class for ColumnLineageDatasetFacetFieldsAdditional
   */
  @JsonDeserialize(
      as = ColumnLineageDatasetFacetFieldsAdditional.class
  )
  @JsonPropertyOrder({
      "inputFields",
      "transformationDescription",
      "transformationType"
  })
  public static final class ColumnLineageDatasetFacetFieldsAdditional {
    private final List<InputField> inputFields;

    private final String transformationDescription;

    private final String transformationType;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param inputFields the inputFields
     * @param transformationDescription a string representation of the transformation applied
     * @param transformationType IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input; MASKED: no original data available (like a hash of PII for example)
     */
    @JsonCreator
    private ColumnLineageDatasetFacetFieldsAdditional(
        @JsonProperty("inputFields") List<InputField> inputFields,
        @JsonProperty("transformationDescription") String transformationDescription,
        @JsonProperty("transformationType") String transformationType) {
      this.inputFields = inputFields;
      this.transformationDescription = transformationDescription;
      this.transformationType = transformationType;
      this.additionalProperties = new LinkedHashMap<>();
    }

    public List<InputField> getInputFields() {
      return inputFields;
    }

    /**
     * @return a string representation of the transformation applied
     */
    public String getTransformationDescription() {
      return transformationDescription;
    }

    /**
     * @return IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input; MASKED: no original data available (like a hash of PII for example)
     */
    public String getTransformationType() {
      return transformationType;
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
      ColumnLineageDatasetFacetFieldsAdditional that = (ColumnLineageDatasetFacetFieldsAdditional) o;
      if (!Objects.equals(inputFields, that.inputFields)) return false;
      if (!Objects.equals(transformationDescription, that.transformationDescription)) return false;
      if (!Objects.equals(transformationType, that.transformationType)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(inputFields, transformationDescription, transformationType, additionalProperties);
    }
  }

  /**
   * builder class for ColumnLineageDatasetFacetFieldsAdditional
   */
  public static final class ColumnLineageDatasetFacetFieldsAdditionalBuilder implements Builder<ColumnLineageDatasetFacetFieldsAdditional> {
    private List<InputField> inputFields;

    private String transformationDescription;

    private String transformationType;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param inputFields the inputFields
     * @return this
     */
    public ColumnLineageDatasetFacetFieldsAdditionalBuilder inputFields(
        List<InputField> inputFields) {
      this.inputFields = inputFields;
      return this;
    }

    /**
     * @param transformationDescription a string representation of the transformation applied
     * @return this
     */
    public ColumnLineageDatasetFacetFieldsAdditionalBuilder transformationDescription(
        String transformationDescription) {
      this.transformationDescription = transformationDescription;
      return this;
    }

    /**
     * @param transformationType IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input; MASKED: no original data available (like a hash of PII for example)
     * @return this
     */
    public ColumnLineageDatasetFacetFieldsAdditionalBuilder transformationType(
        String transformationType) {
      this.transformationType = transformationType;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ColumnLineageDatasetFacetFieldsAdditionalBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ColumnLineageDatasetFacetFieldsAdditional from the fields set in the builder
     */
    @Override
    public ColumnLineageDatasetFacetFieldsAdditional build() {
      ColumnLineageDatasetFacetFieldsAdditional __result = new ColumnLineageDatasetFacetFieldsAdditional(inputFields, transformationDescription, transformationType);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageRunFacet
   */
  @JsonDeserialize(
      as = LineageRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "lineage"
  })
  public static final class LineageRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final List<LineageEntry> lineage;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param lineage Lineage entries describing data flow observed during this run. Each entry identifies a target entity and the sources that feed into it.
     */
    @JsonCreator
    private LineageRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("lineage") List<LineageEntry> lineage) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/LineageRunFacet.json#/$defs/LineageRunFacet");
      this.lineage = lineage;
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
     * @return Lineage entries describing data flow observed during this run. Each entry identifies a target entity and the sources that feed into it.
     */
    public List<LineageEntry> getLineage() {
      return lineage;
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
      LineageRunFacet that = (LineageRunFacet) o;
      if (!Objects.equals(lineage, that.lineage)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lineage, additionalProperties);
    }
  }

  /**
   * builder class for LineageRunFacet
   */
  public final class LineageRunFacetBuilder implements Builder<LineageRunFacet> {
    private List<LineageEntry> lineage;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param lineage Lineage entries describing data flow observed during this run. Each entry identifies a target entity and the sources that feed into it.
     * @return this
     */
    public LineageRunFacetBuilder lineage(List<LineageEntry> lineage) {
      this.lineage = lineage;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageRunFacet from the fields set in the builder
     */
    @Override
    public LineageRunFacet build() {
      LineageRunFacet __result = new LineageRunFacet(OpenLineage.this.producer, lineage);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsInputDatasetFacetColumnMetricsAdditional
   */
  @JsonDeserialize(
      as = DataQualityMetricsInputDatasetFacetColumnMetricsAdditional.class
  )
  @JsonPropertyOrder({
      "nullCount",
      "distinctCount",
      "sum",
      "count",
      "min",
      "max",
      "quantiles"
  })
  public static final class DataQualityMetricsInputDatasetFacetColumnMetricsAdditional {
    private final Long nullCount;

    private final Long distinctCount;

    private final Double sum;

    private final Double count;

    private final Double min;

    private final Double max;

    private final DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles quantiles;

    /**
     * @param nullCount The number of null values in this column for the rows evaluated
     * @param distinctCount The number of distinct values in this column for the rows evaluated
     * @param sum The total sum of values in this column for the rows evaluated
     * @param count The number of values in this column
     * @param min the min
     * @param max the max
     * @param quantiles The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
     */
    @JsonCreator
    private DataQualityMetricsInputDatasetFacetColumnMetricsAdditional(
        @JsonProperty("nullCount") Long nullCount,
        @JsonProperty("distinctCount") Long distinctCount, @JsonProperty("sum") Double sum,
        @JsonProperty("count") Double count, @JsonProperty("min") Double min,
        @JsonProperty("max") Double max,
        @JsonProperty("quantiles") DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles quantiles) {
      this.nullCount = nullCount;
      this.distinctCount = distinctCount;
      this.sum = sum;
      this.count = count;
      this.min = min;
      this.max = max;
      this.quantiles = quantiles;
    }

    /**
     * @return The number of null values in this column for the rows evaluated
     */
    public Long getNullCount() {
      return nullCount;
    }

    /**
     * @return The number of distinct values in this column for the rows evaluated
     */
    public Long getDistinctCount() {
      return distinctCount;
    }

    /**
     * @return The total sum of values in this column for the rows evaluated
     */
    public Double getSum() {
      return sum;
    }

    /**
     * @return The number of values in this column
     */
    public Double getCount() {
      return count;
    }

    public Double getMin() {
      return min;
    }

    public Double getMax() {
      return max;
    }

    /**
     * @return The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles getQuantiles() {
      return quantiles;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataQualityMetricsInputDatasetFacetColumnMetricsAdditional that = (DataQualityMetricsInputDatasetFacetColumnMetricsAdditional) o;
      if (!Objects.equals(nullCount, that.nullCount)) return false;
      if (!Objects.equals(distinctCount, that.distinctCount)) return false;
      if (!Objects.equals(sum, that.sum)) return false;
      if (!Objects.equals(count, that.count)) return false;
      if (!Objects.equals(min, that.min)) return false;
      if (!Objects.equals(max, that.max)) return false;
      if (!Objects.equals(quantiles, that.quantiles)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(nullCount, distinctCount, sum, count, min, max, quantiles);
    }
  }

  /**
   * builder class for DataQualityMetricsInputDatasetFacetColumnMetricsAdditional
   */
  public static final class DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder implements Builder<DataQualityMetricsInputDatasetFacetColumnMetricsAdditional> {
    private Long nullCount;

    private Long distinctCount;

    private Double sum;

    private Double count;

    private Double min;

    private Double max;

    private DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles quantiles;

    /**
     * @param nullCount The number of null values in this column for the rows evaluated
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder nullCount(
        Long nullCount) {
      this.nullCount = nullCount;
      return this;
    }

    /**
     * @param distinctCount The number of distinct values in this column for the rows evaluated
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder distinctCount(
        Long distinctCount) {
      this.distinctCount = distinctCount;
      return this;
    }

    /**
     * @param sum The total sum of values in this column for the rows evaluated
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder sum(Double sum) {
      this.sum = sum;
      return this;
    }

    /**
     * @param count The number of values in this column
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder count(Double count) {
      this.count = count;
      return this;
    }

    /**
     * @param min the min
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder min(Double min) {
      this.min = min;
      return this;
    }

    /**
     * @param max the max
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder max(Double max) {
      this.max = max;
      return this;
    }

    /**
     * @param quantiles The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder quantiles(
        DataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantiles quantiles) {
      this.quantiles = quantiles;
      return this;
    }

    /**
     * build an instance of DataQualityMetricsInputDatasetFacetColumnMetricsAdditional from the fields set in the builder
     */
    @Override
    public DataQualityMetricsInputDatasetFacetColumnMetricsAdditional build() {
      DataQualityMetricsInputDatasetFacetColumnMetricsAdditional __result = new DataQualityMetricsInputDatasetFacetColumnMetricsAdditional(nullCount, distinctCount, sum, count, min, max, quantiles);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsInputDatasetFacet
   */
  @JsonDeserialize(
      as = DataQualityMetricsInputDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "rowCount",
      "bytes",
      "fileCount",
      "lastUpdated",
      "columnMetrics"
  })
  public static final class DataQualityMetricsInputDatasetFacet implements InputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Long rowCount;

    private final Long bytes;

    private final Long fileCount;

    private final ZonedDateTime lastUpdated;

    private final DataQualityMetricsInputDatasetFacetColumnMetrics columnMetrics;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param rowCount The number of rows evaluated
     * @param bytes The size in bytes
     * @param fileCount The number of files evaluated
     * @param lastUpdated The last time the dataset was changed
     * @param columnMetrics The property key is the column name
     */
    @JsonCreator
    private DataQualityMetricsInputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("rowCount") Long rowCount, @JsonProperty("bytes") Long bytes,
        @JsonProperty("fileCount") Long fileCount,
        @JsonProperty("lastUpdated") ZonedDateTime lastUpdated,
        @JsonProperty("columnMetrics") DataQualityMetricsInputDatasetFacetColumnMetrics columnMetrics) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-3/DataQualityMetricsInputDatasetFacet.json#/$defs/DataQualityMetricsInputDatasetFacet");
      this.rowCount = rowCount;
      this.bytes = bytes;
      this.fileCount = fileCount;
      this.lastUpdated = lastUpdated;
      this.columnMetrics = columnMetrics;
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
     * @return The number of rows evaluated
     */
    public Long getRowCount() {
      return rowCount;
    }

    /**
     * @return The size in bytes
     */
    public Long getBytes() {
      return bytes;
    }

    /**
     * @return The number of files evaluated
     */
    public Long getFileCount() {
      return fileCount;
    }

    /**
     * @return The last time the dataset was changed
     */
    public ZonedDateTime getLastUpdated() {
      return lastUpdated;
    }

    /**
     * @return The property key is the column name
     */
    public DataQualityMetricsInputDatasetFacetColumnMetrics getColumnMetrics() {
      return columnMetrics;
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
      DataQualityMetricsInputDatasetFacet that = (DataQualityMetricsInputDatasetFacet) o;
      if (!Objects.equals(rowCount, that.rowCount)) return false;
      if (!Objects.equals(bytes, that.bytes)) return false;
      if (!Objects.equals(fileCount, that.fileCount)) return false;
      if (!Objects.equals(lastUpdated, that.lastUpdated)) return false;
      if (!Objects.equals(columnMetrics, that.columnMetrics)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(rowCount, bytes, fileCount, lastUpdated, columnMetrics, additionalProperties);
    }
  }

  /**
   * builder class for DataQualityMetricsInputDatasetFacet
   */
  public final class DataQualityMetricsInputDatasetFacetBuilder implements Builder<DataQualityMetricsInputDatasetFacet> {
    private Long rowCount;

    private Long bytes;

    private Long fileCount;

    private ZonedDateTime lastUpdated;

    private DataQualityMetricsInputDatasetFacetColumnMetrics columnMetrics;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param rowCount The number of rows evaluated
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetBuilder rowCount(Long rowCount) {
      this.rowCount = rowCount;
      return this;
    }

    /**
     * @param bytes The size in bytes
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetBuilder bytes(Long bytes) {
      this.bytes = bytes;
      return this;
    }

    /**
     * @param fileCount The number of files evaluated
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetBuilder fileCount(Long fileCount) {
      this.fileCount = fileCount;
      return this;
    }

    /**
     * @param lastUpdated The last time the dataset was changed
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetBuilder lastUpdated(ZonedDateTime lastUpdated) {
      this.lastUpdated = lastUpdated;
      return this;
    }

    /**
     * @param columnMetrics The property key is the column name
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetBuilder columnMetrics(
        DataQualityMetricsInputDatasetFacetColumnMetrics columnMetrics) {
      this.columnMetrics = columnMetrics;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DataQualityMetricsInputDatasetFacet from the fields set in the builder
     */
    @Override
    public DataQualityMetricsInputDatasetFacet build() {
      DataQualityMetricsInputDatasetFacet __result = new DataQualityMetricsInputDatasetFacet(OpenLineage.this.producer, rowCount, bytes, fileCount, lastUpdated, columnMetrics);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DocumentationJobFacet
   */
  @JsonDeserialize(
      as = DocumentationJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "description",
      "contentType"
  })
  public static final class DocumentationJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String description;

    private final String contentType;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param description The description of the job.
     * @param contentType MIME type of the description field content.
     */
    @JsonCreator
    private DocumentationJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("description") String description,
        @JsonProperty("contentType") String contentType) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json#/$defs/DocumentationJobFacet");
      this._deleted = null;
      this.description = description;
      this.contentType = contentType;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The description of the job.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return MIME type of the description field content.
     */
    public String getContentType() {
      return contentType;
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
      DocumentationJobFacet that = (DocumentationJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(contentType, that.contentType)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, description, contentType, additionalProperties);
    }
  }

  /**
   * builder class for DocumentationJobFacet
   */
  public final class DocumentationJobFacetBuilder implements Builder<DocumentationJobFacet> {
    private String description;

    private String contentType;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param description The description of the job.
     * @return this
     */
    public DocumentationJobFacetBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param contentType MIME type of the description field content.
     * @return this
     */
    public DocumentationJobFacetBuilder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DocumentationJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DocumentationJobFacet from the fields set in the builder
     */
    @Override
    public DocumentationJobFacet build() {
      DocumentationJobFacet __result = new DocumentationJobFacet(OpenLineage.this.producer, description, contentType);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageTransformation
   */
  @JsonDeserialize(
      as = LineageTransformation.class
  )
  @JsonPropertyOrder({
      "type",
      "subtype",
      "description",
      "masking"
  })
  public static final class LineageTransformation {
    private final String type;

    private final String subtype;

    private final String description;

    private final Boolean masking;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     * @param description A string representation of the transformation applied.
     * @param masking Whether the transformation masks the data (e.g., hashing PII).
     */
    @JsonCreator
    private LineageTransformation(@JsonProperty("type") String type,
        @JsonProperty("subtype") String subtype, @JsonProperty("description") String description,
        @JsonProperty("masking") Boolean masking) {
      this.type = type;
      this.subtype = subtype;
      this.description = description;
      this.masking = masking;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     */
    public String getType() {
      return type;
    }

    /**
     * @return The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     */
    public String getSubtype() {
      return subtype;
    }

    /**
     * @return A string representation of the transformation applied.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return Whether the transformation masks the data (e.g., hashing PII).
     */
    public Boolean getMasking() {
      return masking;
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
      LineageTransformation that = (LineageTransformation) o;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(subtype, that.subtype)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(masking, that.masking)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, subtype, description, masking, additionalProperties);
    }
  }

  /**
   * builder class for LineageTransformation
   */
  public static final class LineageTransformationBuilder implements Builder<LineageTransformation> {
    private String type;

    private String subtype;

    private String description;

    private Boolean masking;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     * @return this
     */
    public LineageTransformationBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     * @return this
     */
    public LineageTransformationBuilder subtype(String subtype) {
      this.subtype = subtype;
      return this;
    }

    /**
     * @param description A string representation of the transformation applied.
     * @return this
     */
    public LineageTransformationBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param masking Whether the transformation masks the data (e.g., hashing PII).
     * @return this
     */
    public LineageTransformationBuilder masking(Boolean masking) {
      this.masking = masking;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageTransformationBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageTransformation from the fields set in the builder
     */
    @Override
    public LineageTransformation build() {
      LineageTransformation __result = new LineageTransformation(type, subtype, description, masking);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for InputDatasetInputFacets
   */
  @JsonDeserialize(
      as = InputDatasetInputFacets.class
  )
  @JsonPropertyOrder({
      "dataQualityAssertions",
      "inputStatistics",
      "dataQualityMetrics",
      "subset",
      "icebergScanReport"
  })
  public static final class InputDatasetInputFacets {
    private final DataQualityAssertionsDatasetFacet dataQualityAssertions;

    private final InputStatisticsInputDatasetFacet inputStatistics;

    private final DataQualityMetricsInputDatasetFacet dataQualityMetrics;

    private final InputSubsetInputDatasetFacet subset;

    private final IcebergScanReportInputDatasetFacet icebergScanReport;

    @JsonAnySetter
    private final Map<String, InputDatasetFacet> additionalProperties;

    /**
     * @param dataQualityAssertions the dataQualityAssertions
     * @param inputStatistics the inputStatistics
     * @param dataQualityMetrics the dataQualityMetrics
     * @param subset the subset
     * @param icebergScanReport the icebergScanReport
     */
    @JsonCreator
    private InputDatasetInputFacets(
        @JsonProperty("dataQualityAssertions") DataQualityAssertionsDatasetFacet dataQualityAssertions,
        @JsonProperty("inputStatistics") InputStatisticsInputDatasetFacet inputStatistics,
        @JsonProperty("dataQualityMetrics") DataQualityMetricsInputDatasetFacet dataQualityMetrics,
        @JsonProperty("subset") InputSubsetInputDatasetFacet subset,
        @JsonProperty("icebergScanReport") IcebergScanReportInputDatasetFacet icebergScanReport) {
      this.dataQualityAssertions = dataQualityAssertions;
      this.inputStatistics = inputStatistics;
      this.dataQualityMetrics = dataQualityMetrics;
      this.subset = subset;
      this.icebergScanReport = icebergScanReport;
      this.additionalProperties = new LinkedHashMap<>();
    }

    public DataQualityAssertionsDatasetFacet getDataQualityAssertions() {
      return dataQualityAssertions;
    }

    public InputStatisticsInputDatasetFacet getInputStatistics() {
      return inputStatistics;
    }

    public DataQualityMetricsInputDatasetFacet getDataQualityMetrics() {
      return dataQualityMetrics;
    }

    public InputSubsetInputDatasetFacet getSubset() {
      return subset;
    }

    public IcebergScanReportInputDatasetFacet getIcebergScanReport() {
      return icebergScanReport;
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
      if (!Objects.equals(dataQualityAssertions, that.dataQualityAssertions)) return false;
      if (!Objects.equals(inputStatistics, that.inputStatistics)) return false;
      if (!Objects.equals(dataQualityMetrics, that.dataQualityMetrics)) return false;
      if (!Objects.equals(subset, that.subset)) return false;
      if (!Objects.equals(icebergScanReport, that.icebergScanReport)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataQualityAssertions, inputStatistics, dataQualityMetrics, subset, icebergScanReport, additionalProperties);
    }
  }

  /**
   * builder class for InputDatasetInputFacets
   */
  public static final class InputDatasetInputFacetsBuilder implements Builder<InputDatasetInputFacets> {
    private DataQualityAssertionsDatasetFacet dataQualityAssertions;

    private InputStatisticsInputDatasetFacet inputStatistics;

    private DataQualityMetricsInputDatasetFacet dataQualityMetrics;

    private InputSubsetInputDatasetFacet subset;

    private IcebergScanReportInputDatasetFacet icebergScanReport;

    private final Map<String, InputDatasetFacet> additionalProperties = new LinkedHashMap<>();

    /**
     * @param dataQualityAssertions the dataQualityAssertions
     * @return this
     */
    public InputDatasetInputFacetsBuilder dataQualityAssertions(
        DataQualityAssertionsDatasetFacet dataQualityAssertions) {
      this.dataQualityAssertions = dataQualityAssertions;
      return this;
    }

    /**
     * @param inputStatistics the inputStatistics
     * @return this
     */
    public InputDatasetInputFacetsBuilder inputStatistics(
        InputStatisticsInputDatasetFacet inputStatistics) {
      this.inputStatistics = inputStatistics;
      return this;
    }

    /**
     * @param dataQualityMetrics the dataQualityMetrics
     * @return this
     */
    public InputDatasetInputFacetsBuilder dataQualityMetrics(
        DataQualityMetricsInputDatasetFacet dataQualityMetrics) {
      this.dataQualityMetrics = dataQualityMetrics;
      return this;
    }

    /**
     * @param subset the subset
     * @return this
     */
    public InputDatasetInputFacetsBuilder subset(InputSubsetInputDatasetFacet subset) {
      this.subset = subset;
      return this;
    }

    /**
     * @param icebergScanReport the icebergScanReport
     * @return this
     */
    public InputDatasetInputFacetsBuilder icebergScanReport(
        IcebergScanReportInputDatasetFacet icebergScanReport) {
      this.icebergScanReport = icebergScanReport;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public InputDatasetInputFacetsBuilder put(String key, InputDatasetFacet value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of InputDatasetInputFacets from the fields set in the builder
     */
    @Override
    public InputDatasetInputFacets build() {
      InputDatasetInputFacets __result = new InputDatasetInputFacets(dataQualityAssertions, inputStatistics, dataQualityMetrics, subset, icebergScanReport);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageDatasetFacet
   */
  @JsonDeserialize(
      as = LineageDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "inputs",
      "fields"
  })
  public static final class LineageDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<LineageDatasetInput> inputs;

    private final LineageDatasetFacetFields fields;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param inputs Dataset-level source inputs that feed into this dataset. When a source includes a 'field' property, it represents a dataset-wide operation (e.g., GROUP BY, FILTER) where that source column affects the entire target dataset.
     * @param fields Column-level lineage. Maps target field names in this dataset to their source inputs.
     */
    @JsonCreator
    private LineageDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("inputs") List<LineageDatasetInput> inputs,
        @JsonProperty("fields") LineageDatasetFacetFields fields) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/LineageDatasetFacet.json#/$defs/LineageDatasetFacet");
      this._deleted = null;
      this.inputs = inputs;
      this.fields = fields;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Dataset-level source inputs that feed into this dataset. When a source includes a 'field' property, it represents a dataset-wide operation (e.g., GROUP BY, FILTER) where that source column affects the entire target dataset.
     */
    public List<LineageDatasetInput> getInputs() {
      return inputs;
    }

    /**
     * @return Column-level lineage. Maps target field names in this dataset to their source inputs.
     */
    public LineageDatasetFacetFields getFields() {
      return fields;
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
      LineageDatasetFacet that = (LineageDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(fields, that.fields)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, inputs, fields, additionalProperties);
    }
  }

  /**
   * builder class for LineageDatasetFacet
   */
  public final class LineageDatasetFacetBuilder implements Builder<LineageDatasetFacet> {
    private List<LineageDatasetInput> inputs;

    private LineageDatasetFacetFields fields;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param inputs Dataset-level source inputs that feed into this dataset. When a source includes a 'field' property, it represents a dataset-wide operation (e.g., GROUP BY, FILTER) where that source column affects the entire target dataset.
     * @return this
     */
    public LineageDatasetFacetBuilder inputs(List<LineageDatasetInput> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * @param fields Column-level lineage. Maps target field names in this dataset to their source inputs.
     * @return this
     */
    public LineageDatasetFacetBuilder fields(LineageDatasetFacetFields fields) {
      this.fields = fields;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageDatasetFacet from the fields set in the builder
     */
    @Override
    public LineageDatasetFacet build() {
      LineageDatasetFacet __result = new LineageDatasetFacet(OpenLineage.this.producer, inputs, fields);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageInput
   */
  @JsonDeserialize(
      as = LineageInput.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "type",
      "field",
      "transformations"
  })
  public static final class LineageInput {
    private final String namespace;

    private final String name;

    private final String type;

    private final String field;

    private final List<LineageTransformation> transformations;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace of the source entity.
     * @param name The name of the source entity.
     * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     * @param transformations Transformations applied to the source data.
     */
    @JsonCreator
    private LineageInput(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("type") String type,
        @JsonProperty("field") String field,
        @JsonProperty("transformations") List<LineageTransformation> transformations) {
      this.namespace = namespace;
      this.name = name;
      this.type = type;
      this.field = field;
      this.transformations = transformations;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace of the source entity.
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The name of the source entity.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     */
    public String getType() {
      return type;
    }

    /**
     * @return The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     */
    public String getField() {
      return field;
    }

    /**
     * @return Transformations applied to the source data.
     */
    public List<LineageTransformation> getTransformations() {
      return transformations;
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
      LineageInput that = (LineageInput) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(field, that.field)) return false;
      if (!Objects.equals(transformations, that.transformations)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, type, field, transformations, additionalProperties);
    }
  }

  /**
   * builder class for LineageInput
   */
  public static final class LineageInputBuilder implements Builder<LineageInput> {
    private String namespace;

    private String name;

    private String type;

    private String field;

    private List<LineageTransformation> transformations;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param namespace The namespace of the source entity.
     * @return this
     */
    public LineageInputBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The name of the source entity.
     * @return this
     */
    public LineageInputBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     * @return this
     */
    public LineageInputBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     * @return this
     */
    public LineageInputBuilder field(String field) {
      this.field = field;
      return this;
    }

    /**
     * @param transformations Transformations applied to the source data.
     * @return this
     */
    public LineageInputBuilder transformations(List<LineageTransformation> transformations) {
      this.transformations = transformations;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageInputBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageInput from the fields set in the builder
     */
    @Override
    public LineageInput build() {
      LineageInput __result = new LineageInput(namespace, name, type, field, transformations);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for SQLJobFacet
   */
  @JsonDeserialize(
      as = SQLJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "query",
      "dialect"
  })
  public static final class SQLJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String query;

    private final String dialect;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param query the query
     * @param dialect the dialect
     */
    @JsonCreator
    private SQLJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("query") String query, @JsonProperty("dialect") String dialect) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json#/$defs/SQLJobFacet");
      this._deleted = null;
      this.query = query;
      this.dialect = dialect;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    public String getQuery() {
      return query;
    }

    public String getDialect() {
      return dialect;
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
      SQLJobFacet that = (SQLJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(query, that.query)) return false;
      if (!Objects.equals(dialect, that.dialect)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, query, dialect, additionalProperties);
    }
  }

  /**
   * builder class for SQLJobFacet
   */
  public final class SQLJobFacetBuilder implements Builder<SQLJobFacet> {
    private String query;

    private String dialect;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param query the query
     * @return this
     */
    public SQLJobFacetBuilder query(String query) {
      this.query = query;
      return this;
    }

    /**
     * @param dialect the dialect
     * @return this
     */
    public SQLJobFacetBuilder dialect(String dialect) {
      this.dialect = dialect;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public SQLJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of SQLJobFacet from the fields set in the builder
     */
    @Override
    public SQLJobFacet build() {
      SQLJobFacet __result = new SQLJobFacet(OpenLineage.this.producer, query, dialect);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageDatasetFacetFields
   */
  @JsonDeserialize(
      as = LineageDatasetFacetFields.class
  )
  @JsonPropertyOrder
  public static final class LineageDatasetFacetFields {
    @JsonAnySetter
    private final Map<String, LineageDatasetFieldEntry> additionalProperties;

    @JsonCreator
    private LineageDatasetFacetFields() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, LineageDatasetFieldEntry> getAdditionalProperties() {
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
      LineageDatasetFacetFields that = (LineageDatasetFacetFields) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for LineageDatasetFacetFields
   */
  public static final class LineageDatasetFacetFieldsBuilder implements Builder<LineageDatasetFacetFields> {
    private final Map<String, LineageDatasetFieldEntry> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageDatasetFacetFieldsBuilder put(String key, LineageDatasetFieldEntry value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageDatasetFacetFields from the fields set in the builder
     */
    @Override
    public LineageDatasetFacetFields build() {
      LineageDatasetFacetFields __result = new LineageDatasetFacetFields();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for GcpComposerJobFacet
   */
  @JsonDeserialize(
      as = GcpComposerJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "environmentName",
      "dagId",
      "operator",
      "taskId",
      "airflowVersion",
      "composerVersion"
  })
  public static final class GcpComposerJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String environmentName;

    private final String dagId;

    private final String operator;

    private final String taskId;

    private final String airflowVersion;

    private final String composerVersion;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param environmentName Cloud Composer Environment name
     * @param dagId The id of the DAG
     * @param operator Operator class name. Only present for tasks, not for DAGs. For example `PythonOperator`
     * @param taskId The id of the task. Only present for tasks, not for DAGs
     * @param airflowVersion Version of Airflow, suffixed by `+composer`
     * @param composerVersion Version of the Cloud Composer environment
     */
    @JsonCreator
    private GcpComposerJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("environmentName") String environmentName,
        @JsonProperty("dagId") String dagId, @JsonProperty("operator") String operator,
        @JsonProperty("taskId") String taskId,
        @JsonProperty("airflowVersion") String airflowVersion,
        @JsonProperty("composerVersion") String composerVersion) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/GcpComposerJobFacet.json#/$defs/GcpComposerJobFacet");
      this._deleted = null;
      this.environmentName = environmentName;
      this.dagId = dagId;
      this.operator = operator;
      this.taskId = taskId;
      this.airflowVersion = airflowVersion;
      this.composerVersion = composerVersion;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Cloud Composer Environment name
     */
    public String getEnvironmentName() {
      return environmentName;
    }

    /**
     * @return The id of the DAG
     */
    public String getDagId() {
      return dagId;
    }

    /**
     * @return Operator class name. Only present for tasks, not for DAGs. For example `PythonOperator`
     */
    public String getOperator() {
      return operator;
    }

    /**
     * @return The id of the task. Only present for tasks, not for DAGs
     */
    public String getTaskId() {
      return taskId;
    }

    /**
     * @return Version of Airflow, suffixed by `+composer`
     */
    public String getAirflowVersion() {
      return airflowVersion;
    }

    /**
     * @return Version of the Cloud Composer environment
     */
    public String getComposerVersion() {
      return composerVersion;
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
      GcpComposerJobFacet that = (GcpComposerJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(environmentName, that.environmentName)) return false;
      if (!Objects.equals(dagId, that.dagId)) return false;
      if (!Objects.equals(operator, that.operator)) return false;
      if (!Objects.equals(taskId, that.taskId)) return false;
      if (!Objects.equals(airflowVersion, that.airflowVersion)) return false;
      if (!Objects.equals(composerVersion, that.composerVersion)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, environmentName, dagId, operator, taskId, airflowVersion, composerVersion, additionalProperties);
    }
  }

  /**
   * builder class for GcpComposerJobFacet
   */
  public final class GcpComposerJobFacetBuilder implements Builder<GcpComposerJobFacet> {
    private String environmentName;

    private String dagId;

    private String operator;

    private String taskId;

    private String airflowVersion;

    private String composerVersion;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param environmentName Cloud Composer Environment name
     * @return this
     */
    public GcpComposerJobFacetBuilder environmentName(String environmentName) {
      this.environmentName = environmentName;
      return this;
    }

    /**
     * @param dagId The id of the DAG
     * @return this
     */
    public GcpComposerJobFacetBuilder dagId(String dagId) {
      this.dagId = dagId;
      return this;
    }

    /**
     * @param operator Operator class name. Only present for tasks, not for DAGs. For example `PythonOperator`
     * @return this
     */
    public GcpComposerJobFacetBuilder operator(String operator) {
      this.operator = operator;
      return this;
    }

    /**
     * @param taskId The id of the task. Only present for tasks, not for DAGs
     * @return this
     */
    public GcpComposerJobFacetBuilder taskId(String taskId) {
      this.taskId = taskId;
      return this;
    }

    /**
     * @param airflowVersion Version of Airflow, suffixed by `+composer`
     * @return this
     */
    public GcpComposerJobFacetBuilder airflowVersion(String airflowVersion) {
      this.airflowVersion = airflowVersion;
      return this;
    }

    /**
     * @param composerVersion Version of the Cloud Composer environment
     * @return this
     */
    public GcpComposerJobFacetBuilder composerVersion(String composerVersion) {
      this.composerVersion = composerVersion;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public GcpComposerJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of GcpComposerJobFacet from the fields set in the builder
     */
    @Override
    public GcpComposerJobFacet build() {
      GcpComposerJobFacet __result = new GcpComposerJobFacet(OpenLineage.this.producer, environmentName, dagId, operator, taskId, airflowVersion, composerVersion);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for OutputDataset
   */
  @JsonDeserialize(
      as = OutputDataset.class
  )
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

    /**
     * @param namespace The namespace containing that dataset
     * @param name The unique name for that dataset within that namespace
     * @param facets The facets for this dataset
     * @param outputFacets The output facets for this dataset
     */
    @JsonCreator
    private OutputDataset(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("facets") DatasetFacets facets,
        @JsonProperty("outputFacets") OutputDatasetOutputFacets outputFacets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
      this.outputFacets = outputFacets;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OutputDataset that = (OutputDataset) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      if (!Objects.equals(outputFacets, that.outputFacets)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets, outputFacets);
    }
  }

  /**
   * builder class for OutputDataset
   */
  public static final class OutputDatasetBuilder implements Builder<OutputDataset> {
    private String namespace;

    private String name;

    private DatasetFacets facets;

    private OutputDatasetOutputFacets outputFacets;

    /**
     * @param namespace The namespace containing that dataset
     * @return this
     */
    public OutputDatasetBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The unique name for that dataset within that namespace
     * @return this
     */
    public OutputDatasetBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param facets The facets for this dataset
     * @return this
     */
    public OutputDatasetBuilder facets(DatasetFacets facets) {
      this.facets = facets;
      return this;
    }

    /**
     * @param outputFacets The output facets for this dataset
     * @return this
     */
    public OutputDatasetBuilder outputFacets(OutputDatasetOutputFacets outputFacets) {
      this.outputFacets = outputFacets;
      return this;
    }

    /**
     * build an instance of OutputDataset from the fields set in the builder
     */
    @Override
    public OutputDataset build() {
      OutputDataset __result = new OutputDataset(namespace, name, facets, outputFacets);
      return __result;
    }
  }

  /**
   * model class for TagsDatasetFacetFields
   */
  @JsonDeserialize(
      as = TagsDatasetFacetFields.class
  )
  @JsonPropertyOrder({
      "key",
      "value",
      "source",
      "field"
  })
  public static final class TagsDatasetFacetFields {
    private final String key;

    private final String value;

    private final String source;

    private final String field;

    /**
     * @param key Key that identifies the tag
     * @param value The value of the field
     * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     * @param field Identifies the field in a dataset if a tag applies to one
     */
    @JsonCreator
    private TagsDatasetFacetFields(@JsonProperty("key") String key,
        @JsonProperty("value") String value, @JsonProperty("source") String source,
        @JsonProperty("field") String field) {
      this.key = key;
      this.value = value;
      this.source = source;
      this.field = field;
    }

    /**
     * @return Key that identifies the tag
     */
    public String getKey() {
      return key;
    }

    /**
     * @return The value of the field
     */
    public String getValue() {
      return value;
    }

    /**
     * @return The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     */
    public String getSource() {
      return source;
    }

    /**
     * @return Identifies the field in a dataset if a tag applies to one
     */
    public String getField() {
      return field;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TagsDatasetFacetFields that = (TagsDatasetFacetFields) o;
      if (!Objects.equals(key, that.key)) return false;
      if (!Objects.equals(value, that.value)) return false;
      if (!Objects.equals(source, that.source)) return false;
      if (!Objects.equals(field, that.field)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, source, field);
    }
  }

  /**
   * builder class for TagsDatasetFacetFields
   */
  public static final class TagsDatasetFacetFieldsBuilder implements Builder<TagsDatasetFacetFields> {
    private String key;

    private String value;

    private String source;

    private String field;

    /**
     * @param key Key that identifies the tag
     * @return this
     */
    public TagsDatasetFacetFieldsBuilder key(String key) {
      this.key = key;
      return this;
    }

    /**
     * @param value The value of the field
     * @return this
     */
    public TagsDatasetFacetFieldsBuilder value(String value) {
      this.value = value;
      return this;
    }

    /**
     * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     * @return this
     */
    public TagsDatasetFacetFieldsBuilder source(String source) {
      this.source = source;
      return this;
    }

    /**
     * @param field Identifies the field in a dataset if a tag applies to one
     * @return this
     */
    public TagsDatasetFacetFieldsBuilder field(String field) {
      this.field = field;
      return this;
    }

    /**
     * build an instance of TagsDatasetFacetFields from the fields set in the builder
     */
    @Override
    public TagsDatasetFacetFields build() {
      TagsDatasetFacetFields __result = new TagsDatasetFacetFields(key, value, source, field);
      return __result;
    }
  }

  /**
   * model class for RootJob
   */
  @JsonDeserialize(
      as = RootJob.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name"
  })
  public static final class RootJob {
    private final String namespace;

    private final String name;

    /**
     * @param namespace The namespace containing root job
     * @param name The unique name containing root job within that namespace
     */
    @JsonCreator
    private RootJob(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name) {
      this.namespace = namespace;
      this.name = name;
    }

    /**
     * @return The namespace containing root job
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The unique name containing root job within that namespace
     */
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RootJob that = (RootJob) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name);
    }
  }

  /**
   * builder class for RootJob
   */
  public static final class RootJobBuilder implements Builder<RootJob> {
    private String namespace;

    private String name;

    /**
     * @param namespace The namespace containing root job
     * @return this
     */
    public RootJobBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The unique name containing root job within that namespace
     * @return this
     */
    public RootJobBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * build an instance of RootJob from the fields set in the builder
     */
    @Override
    public RootJob build() {
      RootJob __result = new RootJob(namespace, name);
      return __result;
    }
  }

  /**
   * model class for LineageEntry
   */
  @JsonDeserialize(
      as = LineageEntry.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "type",
      "inputs",
      "fields"
  })
  public static final class LineageEntry {
    private final String namespace;

    private final String name;

    private final String type;

    private final List<LineageInput> inputs;

    private final LineageEntryFields fields;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace of the target entity.
     * @param name The name of the target entity.
     * @param type The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
     * @param inputs Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
     * @param fields Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
     */
    @JsonCreator
    private LineageEntry(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("type") String type,
        @JsonProperty("inputs") List<LineageInput> inputs,
        @JsonProperty("fields") LineageEntryFields fields) {
      this.namespace = namespace;
      this.name = name;
      this.type = type;
      this.inputs = inputs;
      this.fields = fields;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace of the target entity.
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The name of the target entity.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
     */
    public String getType() {
      return type;
    }

    /**
     * @return Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
     */
    public List<LineageInput> getInputs() {
      return inputs;
    }

    /**
     * @return Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
     */
    public LineageEntryFields getFields() {
      return fields;
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
      LineageEntry that = (LineageEntry) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(fields, that.fields)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, type, inputs, fields, additionalProperties);
    }
  }

  /**
   * builder class for LineageEntry
   */
  public static final class LineageEntryBuilder implements Builder<LineageEntry> {
    private String namespace;

    private String name;

    private String type;

    private List<LineageInput> inputs;

    private LineageEntryFields fields;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param namespace The namespace of the target entity.
     * @return this
     */
    public LineageEntryBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The name of the target entity.
     * @return this
     */
    public LineageEntryBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
     * @return this
     */
    public LineageEntryBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param inputs Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
     * @return this
     */
    public LineageEntryBuilder inputs(List<LineageInput> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * @param fields Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
     * @return this
     */
    public LineageEntryBuilder fields(LineageEntryFields fields) {
      this.fields = fields;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageEntryBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageEntry from the fields set in the builder
     */
    @Override
    public LineageEntry build() {
      LineageEntry __result = new LineageEntry(namespace, name, type, inputs, fields);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageDatasetTransformation
   */
  @JsonDeserialize(
      as = LineageDatasetTransformation.class
  )
  @JsonPropertyOrder({
      "type",
      "subtype",
      "description",
      "masking"
  })
  public static final class LineageDatasetTransformation {
    private final String type;

    private final String subtype;

    private final String description;

    private final Boolean masking;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     * @param description A string representation of the transformation applied.
     * @param masking Whether the transformation masks the data (e.g., hashing PII).
     */
    @JsonCreator
    private LineageDatasetTransformation(@JsonProperty("type") String type,
        @JsonProperty("subtype") String subtype, @JsonProperty("description") String description,
        @JsonProperty("masking") Boolean masking) {
      this.type = type;
      this.subtype = subtype;
      this.description = description;
      this.masking = masking;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     */
    public String getType() {
      return type;
    }

    /**
     * @return The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     */
    public String getSubtype() {
      return subtype;
    }

    /**
     * @return A string representation of the transformation applied.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return Whether the transformation masks the data (e.g., hashing PII).
     */
    public Boolean getMasking() {
      return masking;
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
      LineageDatasetTransformation that = (LineageDatasetTransformation) o;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(subtype, that.subtype)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(masking, that.masking)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, subtype, description, masking, additionalProperties);
    }
  }

  /**
   * builder class for LineageDatasetTransformation
   */
  public static final class LineageDatasetTransformationBuilder implements Builder<LineageDatasetTransformation> {
    private String type;

    private String subtype;

    private String description;

    private Boolean masking;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     * @return this
     */
    public LineageDatasetTransformationBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     * @return this
     */
    public LineageDatasetTransformationBuilder subtype(String subtype) {
      this.subtype = subtype;
      return this;
    }

    /**
     * @param description A string representation of the transformation applied.
     * @return this
     */
    public LineageDatasetTransformationBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param masking Whether the transformation masks the data (e.g., hashing PII).
     * @return this
     */
    public LineageDatasetTransformationBuilder masking(Boolean masking) {
      this.masking = masking;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageDatasetTransformationBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageDatasetTransformation from the fields set in the builder
     */
    @Override
    public LineageDatasetTransformation build() {
      LineageDatasetTransformation __result = new LineageDatasetTransformation(type, subtype, description, masking);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsDatasetFacet
   */
  @JsonDeserialize(
      as = DataQualityMetricsDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "rowCount",
      "bytes",
      "fileCount",
      "lastUpdated",
      "columnMetrics"
  })
  public static final class DataQualityMetricsDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final Long rowCount;

    private final Long bytes;

    private final Long fileCount;

    private final ZonedDateTime lastUpdated;

    private final DataQualityMetricsDatasetFacetColumnMetrics columnMetrics;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param rowCount The number of rows evaluated
     * @param bytes The size in bytes
     * @param fileCount The number of files evaluated
     * @param lastUpdated The last time the dataset was changed
     * @param columnMetrics The property key is the column name
     */
    @JsonCreator
    private DataQualityMetricsDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("rowCount") Long rowCount, @JsonProperty("bytes") Long bytes,
        @JsonProperty("fileCount") Long fileCount,
        @JsonProperty("lastUpdated") ZonedDateTime lastUpdated,
        @JsonProperty("columnMetrics") DataQualityMetricsDatasetFacetColumnMetrics columnMetrics) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsDatasetFacet.json#/$defs/DataQualityMetricsDatasetFacet");
      this._deleted = null;
      this.rowCount = rowCount;
      this.bytes = bytes;
      this.fileCount = fileCount;
      this.lastUpdated = lastUpdated;
      this.columnMetrics = columnMetrics;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The number of rows evaluated
     */
    public Long getRowCount() {
      return rowCount;
    }

    /**
     * @return The size in bytes
     */
    public Long getBytes() {
      return bytes;
    }

    /**
     * @return The number of files evaluated
     */
    public Long getFileCount() {
      return fileCount;
    }

    /**
     * @return The last time the dataset was changed
     */
    public ZonedDateTime getLastUpdated() {
      return lastUpdated;
    }

    /**
     * @return The property key is the column name
     */
    public DataQualityMetricsDatasetFacetColumnMetrics getColumnMetrics() {
      return columnMetrics;
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
      DataQualityMetricsDatasetFacet that = (DataQualityMetricsDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(rowCount, that.rowCount)) return false;
      if (!Objects.equals(bytes, that.bytes)) return false;
      if (!Objects.equals(fileCount, that.fileCount)) return false;
      if (!Objects.equals(lastUpdated, that.lastUpdated)) return false;
      if (!Objects.equals(columnMetrics, that.columnMetrics)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, rowCount, bytes, fileCount, lastUpdated, columnMetrics, additionalProperties);
    }
  }

  /**
   * builder class for DataQualityMetricsDatasetFacet
   */
  public final class DataQualityMetricsDatasetFacetBuilder implements Builder<DataQualityMetricsDatasetFacet> {
    private Long rowCount;

    private Long bytes;

    private Long fileCount;

    private ZonedDateTime lastUpdated;

    private DataQualityMetricsDatasetFacetColumnMetrics columnMetrics;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param rowCount The number of rows evaluated
     * @return this
     */
    public DataQualityMetricsDatasetFacetBuilder rowCount(Long rowCount) {
      this.rowCount = rowCount;
      return this;
    }

    /**
     * @param bytes The size in bytes
     * @return this
     */
    public DataQualityMetricsDatasetFacetBuilder bytes(Long bytes) {
      this.bytes = bytes;
      return this;
    }

    /**
     * @param fileCount The number of files evaluated
     * @return this
     */
    public DataQualityMetricsDatasetFacetBuilder fileCount(Long fileCount) {
      this.fileCount = fileCount;
      return this;
    }

    /**
     * @param lastUpdated The last time the dataset was changed
     * @return this
     */
    public DataQualityMetricsDatasetFacetBuilder lastUpdated(ZonedDateTime lastUpdated) {
      this.lastUpdated = lastUpdated;
      return this;
    }

    /**
     * @param columnMetrics The property key is the column name
     * @return this
     */
    public DataQualityMetricsDatasetFacetBuilder columnMetrics(
        DataQualityMetricsDatasetFacetColumnMetrics columnMetrics) {
      this.columnMetrics = columnMetrics;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DataQualityMetricsDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DataQualityMetricsDatasetFacet from the fields set in the builder
     */
    @Override
    public DataQualityMetricsDatasetFacet build() {
      DataQualityMetricsDatasetFacet __result = new DataQualityMetricsDatasetFacet(OpenLineage.this.producer, rowCount, bytes, fileCount, lastUpdated, columnMetrics);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DocumentationDatasetFacet
   */
  @JsonDeserialize(
      as = DocumentationDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "description",
      "contentType"
  })
  public static final class DocumentationDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String description;

    private final String contentType;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param description The description of the dataset.
     * @param contentType MIME type of the description field content.
     */
    @JsonCreator
    private DocumentationDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("description") String description,
        @JsonProperty("contentType") String contentType) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-0/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet");
      this._deleted = null;
      this.description = description;
      this.contentType = contentType;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The description of the dataset.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return MIME type of the description field content.
     */
    public String getContentType() {
      return contentType;
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
      DocumentationDatasetFacet that = (DocumentationDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(contentType, that.contentType)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, description, contentType, additionalProperties);
    }
  }

  /**
   * builder class for DocumentationDatasetFacet
   */
  public final class DocumentationDatasetFacetBuilder implements Builder<DocumentationDatasetFacet> {
    private String description;

    private String contentType;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param description The description of the dataset.
     * @return this
     */
    public DocumentationDatasetFacetBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param contentType MIME type of the description field content.
     * @return this
     */
    public DocumentationDatasetFacetBuilder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DocumentationDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DocumentationDatasetFacet from the fields set in the builder
     */
    @Override
    public DocumentationDatasetFacet build() {
      DocumentationDatasetFacet __result = new DocumentationDatasetFacet(OpenLineage.this.producer, description, contentType);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for SourceCodeLocationJobFacet
   */
  @JsonDeserialize(
      as = SourceCodeLocationJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "type",
      "url",
      "repoUrl",
      "path",
      "version",
      "tag",
      "branch",
      "pullRequestNumber"
  })
  public static final class SourceCodeLocationJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String type;

    private final URI url;

    private final String repoUrl;

    private final String path;

    private final String version;

    private final String tag;

    private final String branch;

    private final String pullRequestNumber;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param type the source control system
     * @param url the full http URL to locate the file
     * @param repoUrl the URL to the repository
     * @param path the path in the repo containing the source files
     * @param version the current version deployed (not a branch name, the actual unique version)
     * @param tag optional tag name
     * @param branch optional branch name
     * @param pullRequestNumber optional pull request or merge request number associated with a CI run, populated from CI platform environment variables (e.g. GITHUB_REF, CI_MERGE_REQUEST_IID)
     */
    @JsonCreator
    private SourceCodeLocationJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("type") String type, @JsonProperty("url") URI url,
        @JsonProperty("repoUrl") String repoUrl, @JsonProperty("path") String path,
        @JsonProperty("version") String version, @JsonProperty("tag") String tag,
        @JsonProperty("branch") String branch,
        @JsonProperty("pullRequestNumber") String pullRequestNumber) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-0/SourceCodeLocationJobFacet.json#/$defs/SourceCodeLocationJobFacet");
      this._deleted = null;
      this.type = type;
      this.url = url;
      this.repoUrl = repoUrl;
      this.path = path;
      this.version = version;
      this.tag = tag;
      this.branch = branch;
      this.pullRequestNumber = pullRequestNumber;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return the source control system
     */
    public String getType() {
      return type;
    }

    /**
     * @return the full http URL to locate the file
     */
    public URI getUrl() {
      return url;
    }

    /**
     * @return the URL to the repository
     */
    public String getRepoUrl() {
      return repoUrl;
    }

    /**
     * @return the path in the repo containing the source files
     */
    public String getPath() {
      return path;
    }

    /**
     * @return the current version deployed (not a branch name, the actual unique version)
     */
    public String getVersion() {
      return version;
    }

    /**
     * @return optional tag name
     */
    public String getTag() {
      return tag;
    }

    /**
     * @return optional branch name
     */
    public String getBranch() {
      return branch;
    }

    /**
     * @return optional pull request or merge request number associated with a CI run, populated from CI platform environment variables (e.g. GITHUB_REF, CI_MERGE_REQUEST_IID)
     */
    public String getPullRequestNumber() {
      return pullRequestNumber;
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
      SourceCodeLocationJobFacet that = (SourceCodeLocationJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(url, that.url)) return false;
      if (!Objects.equals(repoUrl, that.repoUrl)) return false;
      if (!Objects.equals(path, that.path)) return false;
      if (!Objects.equals(version, that.version)) return false;
      if (!Objects.equals(tag, that.tag)) return false;
      if (!Objects.equals(branch, that.branch)) return false;
      if (!Objects.equals(pullRequestNumber, that.pullRequestNumber)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, type, url, repoUrl, path, version, tag, branch, pullRequestNumber, additionalProperties);
    }
  }

  /**
   * builder class for SourceCodeLocationJobFacet
   */
  public final class SourceCodeLocationJobFacetBuilder implements Builder<SourceCodeLocationJobFacet> {
    private String type;

    private URI url;

    private String repoUrl;

    private String path;

    private String version;

    private String tag;

    private String branch;

    private String pullRequestNumber;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param type the source control system
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param url the full http URL to locate the file
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder url(URI url) {
      this.url = url;
      return this;
    }

    /**
     * @param repoUrl the URL to the repository
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder repoUrl(String repoUrl) {
      this.repoUrl = repoUrl;
      return this;
    }

    /**
     * @param path the path in the repo containing the source files
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder path(String path) {
      this.path = path;
      return this;
    }

    /**
     * @param version the current version deployed (not a branch name, the actual unique version)
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder version(String version) {
      this.version = version;
      return this;
    }

    /**
     * @param tag optional tag name
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder tag(String tag) {
      this.tag = tag;
      return this;
    }

    /**
     * @param branch optional branch name
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder branch(String branch) {
      this.branch = branch;
      return this;
    }

    /**
     * @param pullRequestNumber optional pull request or merge request number associated with a CI run, populated from CI platform environment variables (e.g. GITHUB_REF, CI_MERGE_REQUEST_IID)
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder pullRequestNumber(String pullRequestNumber) {
      this.pullRequestNumber = pullRequestNumber;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public SourceCodeLocationJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of SourceCodeLocationJobFacet from the fields set in the builder
     */
    @Override
    public SourceCodeLocationJobFacet build() {
      SourceCodeLocationJobFacet __result = new SourceCodeLocationJobFacet(OpenLineage.this.producer, type, url, repoUrl, path, version, tag, branch, pullRequestNumber);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for IcebergScanReportInputDatasetFacet
   */
  @JsonDeserialize(
      as = IcebergScanReportInputDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "snapshotId",
      "filterDescription",
      "schemaId",
      "projectedFieldNames",
      "scanMetrics",
      "metadata"
  })
  public static final class IcebergScanReportInputDatasetFacet implements InputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Double snapshotId;

    private final String filterDescription;

    private final Double schemaId;

    private final List<String> projectedFieldNames;

    private final IcebergScanReportInputDatasetFacetScanMetrics scanMetrics;

    private final IcebergScanReportInputDatasetFacetMetadata metadata;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param snapshotId Snapshot ID of the iceberg table
     * @param filterDescription Filter used to scan the iceberg table
     * @param schemaId Schema ID of the iceberg table
     * @param projectedFieldNames List of field names that are projected from the iceberg table
     * @param scanMetrics the scanMetrics
     * @param metadata the metadata
     */
    @JsonCreator
    private IcebergScanReportInputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("snapshotId") Double snapshotId,
        @JsonProperty("filterDescription") String filterDescription,
        @JsonProperty("schemaId") Double schemaId,
        @JsonProperty("projectedFieldNames") List<String> projectedFieldNames,
        @JsonProperty("scanMetrics") IcebergScanReportInputDatasetFacetScanMetrics scanMetrics,
        @JsonProperty("metadata") IcebergScanReportInputDatasetFacetMetadata metadata) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/IcebergScanReportInputDatasetFacet.json#/$defs/IcebergScanReportInputDatasetFacet");
      this.snapshotId = snapshotId;
      this.filterDescription = filterDescription;
      this.schemaId = schemaId;
      this.projectedFieldNames = projectedFieldNames;
      this.scanMetrics = scanMetrics;
      this.metadata = metadata;
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
     * @return Snapshot ID of the iceberg table
     */
    public Double getSnapshotId() {
      return snapshotId;
    }

    /**
     * @return Filter used to scan the iceberg table
     */
    public String getFilterDescription() {
      return filterDescription;
    }

    /**
     * @return Schema ID of the iceberg table
     */
    public Double getSchemaId() {
      return schemaId;
    }

    /**
     * @return List of field names that are projected from the iceberg table
     */
    public List<String> getProjectedFieldNames() {
      return projectedFieldNames;
    }

    public IcebergScanReportInputDatasetFacetScanMetrics getScanMetrics() {
      return scanMetrics;
    }

    public IcebergScanReportInputDatasetFacetMetadata getMetadata() {
      return metadata;
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
      IcebergScanReportInputDatasetFacet that = (IcebergScanReportInputDatasetFacet) o;
      if (!Objects.equals(snapshotId, that.snapshotId)) return false;
      if (!Objects.equals(filterDescription, that.filterDescription)) return false;
      if (!Objects.equals(schemaId, that.schemaId)) return false;
      if (!Objects.equals(projectedFieldNames, that.projectedFieldNames)) return false;
      if (!Objects.equals(scanMetrics, that.scanMetrics)) return false;
      if (!Objects.equals(metadata, that.metadata)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(snapshotId, filterDescription, schemaId, projectedFieldNames, scanMetrics, metadata, additionalProperties);
    }
  }

  /**
   * builder class for IcebergScanReportInputDatasetFacet
   */
  public final class IcebergScanReportInputDatasetFacetBuilder implements Builder<IcebergScanReportInputDatasetFacet> {
    private Double snapshotId;

    private String filterDescription;

    private Double schemaId;

    private List<String> projectedFieldNames;

    private IcebergScanReportInputDatasetFacetScanMetrics scanMetrics;

    private IcebergScanReportInputDatasetFacetMetadata metadata;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param snapshotId Snapshot ID of the iceberg table
     * @return this
     */
    public IcebergScanReportInputDatasetFacetBuilder snapshotId(Double snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    /**
     * @param filterDescription Filter used to scan the iceberg table
     * @return this
     */
    public IcebergScanReportInputDatasetFacetBuilder filterDescription(String filterDescription) {
      this.filterDescription = filterDescription;
      return this;
    }

    /**
     * @param schemaId Schema ID of the iceberg table
     * @return this
     */
    public IcebergScanReportInputDatasetFacetBuilder schemaId(Double schemaId) {
      this.schemaId = schemaId;
      return this;
    }

    /**
     * @param projectedFieldNames List of field names that are projected from the iceberg table
     * @return this
     */
    public IcebergScanReportInputDatasetFacetBuilder projectedFieldNames(
        List<String> projectedFieldNames) {
      this.projectedFieldNames = projectedFieldNames;
      return this;
    }

    /**
     * @param scanMetrics the scanMetrics
     * @return this
     */
    public IcebergScanReportInputDatasetFacetBuilder scanMetrics(
        IcebergScanReportInputDatasetFacetScanMetrics scanMetrics) {
      this.scanMetrics = scanMetrics;
      return this;
    }

    /**
     * @param metadata the metadata
     * @return this
     */
    public IcebergScanReportInputDatasetFacetBuilder metadata(
        IcebergScanReportInputDatasetFacetMetadata metadata) {
      this.metadata = metadata;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public IcebergScanReportInputDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of IcebergScanReportInputDatasetFacet from the fields set in the builder
     */
    @Override
    public IcebergScanReportInputDatasetFacet build() {
      IcebergScanReportInputDatasetFacet __result = new IcebergScanReportInputDatasetFacet(OpenLineage.this.producer, snapshotId, filterDescription, schemaId, projectedFieldNames, scanMetrics, metadata);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
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
  }

  /**
   * model class for DatasetTypeDatasetFacet
   */
  @JsonDeserialize(
      as = DatasetTypeDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "datasetType",
      "subType"
  })
  public static final class DatasetTypeDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String datasetType;

    private final String subType;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param datasetType Dataset type, for example: TABLE|VIEW|FILE|TOPIC|STREAM|MODEL|JOB_OUTPUT.
     * @param subType Optional sub-type within the dataset type (e.g., MATERIALIZED, EXTERNAL, TEMPORARY).
     */
    @JsonCreator
    private DatasetTypeDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("datasetType") String datasetType, @JsonProperty("subType") String subType) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/DatasetTypeDatasetFacet.json#/$defs/DatasetTypeDatasetFacet");
      this._deleted = null;
      this.datasetType = datasetType;
      this.subType = subType;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Dataset type, for example: TABLE|VIEW|FILE|TOPIC|STREAM|MODEL|JOB_OUTPUT.
     */
    public String getDatasetType() {
      return datasetType;
    }

    /**
     * @return Optional sub-type within the dataset type (e.g., MATERIALIZED, EXTERNAL, TEMPORARY).
     */
    public String getSubType() {
      return subType;
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
      DatasetTypeDatasetFacet that = (DatasetTypeDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(datasetType, that.datasetType)) return false;
      if (!Objects.equals(subType, that.subType)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, datasetType, subType, additionalProperties);
    }
  }

  /**
   * builder class for DatasetTypeDatasetFacet
   */
  public final class DatasetTypeDatasetFacetBuilder implements Builder<DatasetTypeDatasetFacet> {
    private String datasetType;

    private String subType;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param datasetType Dataset type, for example: TABLE|VIEW|FILE|TOPIC|STREAM|MODEL|JOB_OUTPUT.
     * @return this
     */
    public DatasetTypeDatasetFacetBuilder datasetType(String datasetType) {
      this.datasetType = datasetType;
      return this;
    }

    /**
     * @param subType Optional sub-type within the dataset type (e.g., MATERIALIZED, EXTERNAL, TEMPORARY).
     * @return this
     */
    public DatasetTypeDatasetFacetBuilder subType(String subType) {
      this.subType = subType;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DatasetTypeDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DatasetTypeDatasetFacet from the fields set in the builder
     */
    @Override
    public DatasetTypeDatasetFacet build() {
      DatasetTypeDatasetFacet __result = new DatasetTypeDatasetFacet(OpenLineage.this.producer, datasetType, subType);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for PartitionSubsetConditionPartitions
   */
  @JsonDeserialize(
      as = PartitionSubsetConditionPartitions.class
  )
  @JsonPropertyOrder({
      "identifier",
      "dimensions"
  })
  public static final class PartitionSubsetConditionPartitions {
    private final String identifier;

    private final PartitionSubsetConditionPartitionsDimensions dimensions;

    /**
     * @param identifier Optionally provided identifier of the partition specified
     * @param dimensions the dimensions
     */
    @JsonCreator
    private PartitionSubsetConditionPartitions(@JsonProperty("identifier") String identifier,
        @JsonProperty("dimensions") PartitionSubsetConditionPartitionsDimensions dimensions) {
      this.identifier = identifier;
      this.dimensions = dimensions;
    }

    /**
     * @return Optionally provided identifier of the partition specified
     */
    public String getIdentifier() {
      return identifier;
    }

    public PartitionSubsetConditionPartitionsDimensions getDimensions() {
      return dimensions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PartitionSubsetConditionPartitions that = (PartitionSubsetConditionPartitions) o;
      if (!Objects.equals(identifier, that.identifier)) return false;
      if (!Objects.equals(dimensions, that.dimensions)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, dimensions);
    }
  }

  /**
   * builder class for PartitionSubsetConditionPartitions
   */
  public static final class PartitionSubsetConditionPartitionsBuilder implements Builder<PartitionSubsetConditionPartitions> {
    private String identifier;

    private PartitionSubsetConditionPartitionsDimensions dimensions;

    /**
     * @param identifier Optionally provided identifier of the partition specified
     * @return this
     */
    public PartitionSubsetConditionPartitionsBuilder identifier(String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @param dimensions the dimensions
     * @return this
     */
    public PartitionSubsetConditionPartitionsBuilder dimensions(
        PartitionSubsetConditionPartitionsDimensions dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    /**
     * build an instance of PartitionSubsetConditionPartitions from the fields set in the builder
     */
    @Override
    public PartitionSubsetConditionPartitions build() {
      PartitionSubsetConditionPartitions __result = new PartitionSubsetConditionPartitions(identifier, dimensions);
      return __result;
    }
  }

  /**
   * model class for Job
   */
  @JsonDeserialize(
      as = Job.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "facets"
  })
  public static final class Job {
    private final String namespace;

    private final String name;

    private final JobFacets facets;

    /**
     * @param namespace The namespace containing that job
     * @param name The unique name for that job within that namespace
     * @param facets The job facets.
     */
    @JsonCreator
    private Job(@JsonProperty("namespace") String namespace, @JsonProperty("name") String name,
        @JsonProperty("facets") JobFacets facets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Job that = (Job) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets);
    }
  }

  /**
   * builder class for Job
   */
  public static final class JobBuilder implements Builder<Job> {
    private String namespace;

    private String name;

    private JobFacets facets;

    /**
     * @param namespace The namespace containing that job
     * @return this
     */
    public JobBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The unique name for that job within that namespace
     * @return this
     */
    public JobBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param facets The job facets.
     * @return this
     */
    public JobBuilder facets(JobFacets facets) {
      this.facets = facets;
      return this;
    }

    /**
     * build an instance of Job from the fields set in the builder
     */
    @Override
    public Job build() {
      Job __result = new Job(namespace, name, facets);
      return __result;
    }
  }

  /**
   * model class for DatasetVersionDatasetFacet
   */
  @JsonDeserialize(
      as = DatasetVersionDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "datasetVersion"
  })
  public static final class DatasetVersionDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String datasetVersion;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param datasetVersion The version of the dataset.
     */
    @JsonCreator
    private DatasetVersionDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("datasetVersion") String datasetVersion) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet");
      this._deleted = null;
      this.datasetVersion = datasetVersion;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The version of the dataset.
     */
    public String getDatasetVersion() {
      return datasetVersion;
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
      DatasetVersionDatasetFacet that = (DatasetVersionDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(datasetVersion, that.datasetVersion)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, datasetVersion, additionalProperties);
    }
  }

  /**
   * builder class for DatasetVersionDatasetFacet
   */
  public final class DatasetVersionDatasetFacetBuilder implements Builder<DatasetVersionDatasetFacet> {
    private String datasetVersion;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param datasetVersion The version of the dataset.
     * @return this
     */
    public DatasetVersionDatasetFacetBuilder datasetVersion(String datasetVersion) {
      this.datasetVersion = datasetVersion;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DatasetVersionDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DatasetVersionDatasetFacet from the fields set in the builder
     */
    @Override
    public DatasetVersionDatasetFacet build() {
      DatasetVersionDatasetFacet __result = new DatasetVersionDatasetFacet(OpenLineage.this.producer, datasetVersion);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for SymlinksDatasetFacetIdentifiers
   */
  @JsonDeserialize(
      as = SymlinksDatasetFacetIdentifiers.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "type"
  })
  public static final class SymlinksDatasetFacetIdentifiers {
    private final String namespace;

    private final String name;

    private final String type;

    /**
     * @param namespace The dataset namespace
     * @param name The dataset name
     * @param type Identifier type
     */
    @JsonCreator
    private SymlinksDatasetFacetIdentifiers(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("type") String type) {
      this.namespace = namespace;
      this.name = name;
      this.type = type;
    }

    /**
     * @return The dataset namespace
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The dataset name
     */
    public String getName() {
      return name;
    }

    /**
     * @return Identifier type
     */
    public String getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SymlinksDatasetFacetIdentifiers that = (SymlinksDatasetFacetIdentifiers) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, type);
    }
  }

  /**
   * builder class for SymlinksDatasetFacetIdentifiers
   */
  public static final class SymlinksDatasetFacetIdentifiersBuilder implements Builder<SymlinksDatasetFacetIdentifiers> {
    private String namespace;

    private String name;

    private String type;

    /**
     * @param namespace The dataset namespace
     * @return this
     */
    public SymlinksDatasetFacetIdentifiersBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The dataset name
     * @return this
     */
    public SymlinksDatasetFacetIdentifiersBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type Identifier type
     * @return this
     */
    public SymlinksDatasetFacetIdentifiersBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * build an instance of SymlinksDatasetFacetIdentifiers from the fields set in the builder
     */
    @Override
    public SymlinksDatasetFacetIdentifiers build() {
      SymlinksDatasetFacetIdentifiers __result = new SymlinksDatasetFacetIdentifiers(namespace, name, type);
      return __result;
    }
  }

  /**
   * model class for LineageJobEntry
   */
  @JsonDeserialize(
      as = LineageJobEntry.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "type",
      "inputs",
      "fields"
  })
  public static final class LineageJobEntry {
    private final String namespace;

    private final String name;

    private final String type;

    private final List<LineageJobInput> inputs;

    private final LineageJobEntryFields fields;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace of the target entity.
     * @param name The name of the target entity.
     * @param type The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
     * @param inputs Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
     * @param fields Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
     */
    @JsonCreator
    private LineageJobEntry(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("type") String type,
        @JsonProperty("inputs") List<LineageJobInput> inputs,
        @JsonProperty("fields") LineageJobEntryFields fields) {
      this.namespace = namespace;
      this.name = name;
      this.type = type;
      this.inputs = inputs;
      this.fields = fields;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace of the target entity.
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The name of the target entity.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
     */
    public String getType() {
      return type;
    }

    /**
     * @return Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
     */
    public List<LineageJobInput> getInputs() {
      return inputs;
    }

    /**
     * @return Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
     */
    public LineageJobEntryFields getFields() {
      return fields;
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
      LineageJobEntry that = (LineageJobEntry) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(fields, that.fields)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, type, inputs, fields, additionalProperties);
    }
  }

  /**
   * builder class for LineageJobEntry
   */
  public static final class LineageJobEntryBuilder implements Builder<LineageJobEntry> {
    private String namespace;

    private String name;

    private String type;

    private List<LineageJobInput> inputs;

    private LineageJobEntryFields fields;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param namespace The namespace of the target entity.
     * @return this
     */
    public LineageJobEntryBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The name of the target entity.
     * @return this
     */
    public LineageJobEntryBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets (sink) or no input datasets (generator).
     * @return this
     */
    public LineageJobEntryBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param inputs Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g., a data generator).
     * @return this
     */
    public LineageJobEntryBuilder inputs(List<LineageJobInput> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * @param fields Column-level lineage. Maps target field names to their source inputs. Only meaningful when the target type is DATASET.
     * @return this
     */
    public LineageJobEntryBuilder fields(LineageJobEntryFields fields) {
      this.fields = fields;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageJobEntryBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageJobEntry from the fields set in the builder
     */
    @Override
    public LineageJobEntry build() {
      LineageJobEntry __result = new LineageJobEntry(namespace, name, type, inputs, fields);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for TagsJobFacetFields
   */
  @JsonDeserialize(
      as = TagsJobFacetFields.class
  )
  @JsonPropertyOrder({
      "key",
      "value",
      "source"
  })
  public static final class TagsJobFacetFields {
    private final String key;

    private final String value;

    private final String source;

    /**
     * @param key Key that identifies the tag
     * @param value The value of the field
     * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     */
    @JsonCreator
    private TagsJobFacetFields(@JsonProperty("key") String key, @JsonProperty("value") String value,
        @JsonProperty("source") String source) {
      this.key = key;
      this.value = value;
      this.source = source;
    }

    /**
     * @return Key that identifies the tag
     */
    public String getKey() {
      return key;
    }

    /**
     * @return The value of the field
     */
    public String getValue() {
      return value;
    }

    /**
     * @return The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     */
    public String getSource() {
      return source;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TagsJobFacetFields that = (TagsJobFacetFields) o;
      if (!Objects.equals(key, that.key)) return false;
      if (!Objects.equals(value, that.value)) return false;
      if (!Objects.equals(source, that.source)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, source);
    }
  }

  /**
   * builder class for TagsJobFacetFields
   */
  public static final class TagsJobFacetFieldsBuilder implements Builder<TagsJobFacetFields> {
    private String key;

    private String value;

    private String source;

    /**
     * @param key Key that identifies the tag
     * @return this
     */
    public TagsJobFacetFieldsBuilder key(String key) {
      this.key = key;
      return this;
    }

    /**
     * @param value The value of the field
     * @return this
     */
    public TagsJobFacetFieldsBuilder value(String value) {
      this.value = value;
      return this;
    }

    /**
     * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     * @return this
     */
    public TagsJobFacetFieldsBuilder source(String source) {
      this.source = source;
      return this;
    }

    /**
     * build an instance of TagsJobFacetFields from the fields set in the builder
     */
    @Override
    public TagsJobFacetFields build() {
      TagsJobFacetFields __result = new TagsJobFacetFields(key, value, source);
      return __result;
    }
  }

  public static class DefaultOutputDatasetFacet implements OutputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @JsonCreator
    public DefaultOutputDatasetFacet(@JsonProperty("_producer") URI _producer) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/OutputDatasetFacet");
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
  }

  /**
   * Interface for OutputDatasetFacet
   */
  @JsonDeserialize(
      as = DefaultOutputDatasetFacet.class
  )
  public interface OutputDatasetFacet {
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
   * model class for StaticDataset
   */
  @JsonDeserialize(
      as = StaticDataset.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "facets"
  })
  public static final class StaticDataset implements Dataset {
    private final String namespace;

    private final String name;

    private final DatasetFacets facets;

    /**
     * @param namespace The namespace containing that dataset
     * @param name The unique name for that dataset within that namespace
     * @param facets The facets for this dataset
     */
    @JsonCreator
    private StaticDataset(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("facets") DatasetFacets facets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StaticDataset that = (StaticDataset) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets);
    }
  }

  /**
   * builder class for StaticDataset
   */
  public static final class StaticDatasetBuilder implements Builder<StaticDataset> {
    private String namespace;

    private String name;

    private DatasetFacets facets;

    /**
     * @param namespace The namespace containing that dataset
     * @return this
     */
    public StaticDatasetBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The unique name for that dataset within that namespace
     * @return this
     */
    public StaticDatasetBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param facets The facets for this dataset
     * @return this
     */
    public StaticDatasetBuilder facets(DatasetFacets facets) {
      this.facets = facets;
      return this;
    }

    /**
     * build an instance of StaticDataset from the fields set in the builder
     */
    @Override
    public StaticDataset build() {
      StaticDataset __result = new StaticDataset(namespace, name, facets);
      return __result;
    }
  }

  /**
   * model class for LineageJobEntryFields
   */
  @JsonDeserialize(
      as = LineageJobEntryFields.class
  )
  @JsonPropertyOrder
  public static final class LineageJobEntryFields {
    @JsonAnySetter
    private final Map<String, LineageJobFieldEntry> additionalProperties;

    @JsonCreator
    private LineageJobEntryFields() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, LineageJobFieldEntry> getAdditionalProperties() {
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
      LineageJobEntryFields that = (LineageJobEntryFields) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for LineageJobEntryFields
   */
  public static final class LineageJobEntryFieldsBuilder implements Builder<LineageJobEntryFields> {
    private final Map<String, LineageJobFieldEntry> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageJobEntryFieldsBuilder put(String key, LineageJobFieldEntry value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageJobEntryFields from the fields set in the builder
     */
    @Override
    public LineageJobEntryFields build() {
      LineageJobEntryFields __result = new LineageJobEntryFields();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for PartitionSubsetConditionPartitionsDimensions
   */
  @JsonDeserialize(
      as = PartitionSubsetConditionPartitionsDimensions.class
  )
  @JsonPropertyOrder
  public static final class PartitionSubsetConditionPartitionsDimensions {
    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    @JsonCreator
    private PartitionSubsetConditionPartitionsDimensions() {
      this.additionalProperties = new LinkedHashMap<>();
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
      PartitionSubsetConditionPartitionsDimensions that = (PartitionSubsetConditionPartitionsDimensions) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for PartitionSubsetConditionPartitionsDimensions
   */
  public static final class PartitionSubsetConditionPartitionsDimensionsBuilder implements Builder<PartitionSubsetConditionPartitionsDimensions> {
    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public PartitionSubsetConditionPartitionsDimensionsBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of PartitionSubsetConditionPartitionsDimensions from the fields set in the builder
     */
    @Override
    public PartitionSubsetConditionPartitionsDimensions build() {
      PartitionSubsetConditionPartitionsDimensions __result = new PartitionSubsetConditionPartitionsDimensions();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for CompareSubsetCondition
   */
  @JsonDeserialize(
      as = CompareSubsetCondition.class
  )
  @JsonPropertyOrder({
      "type",
      "left",
      "right",
      "comparison"
  })
  public static final class CompareSubsetCondition {
    private final String type;

    private final FieldBaseCompareExpression left;

    private final FieldBaseCompareExpression right;

    private final String comparison;

    /**
     * @param left the left
     * @param right the right
     * @param comparison Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN', 'LESS_EQUAL_THAN'
     */
    @JsonCreator
    private CompareSubsetCondition(@JsonProperty("left") FieldBaseCompareExpression left,
        @JsonProperty("right") FieldBaseCompareExpression right,
        @JsonProperty("comparison") String comparison) {
      this.type = "compare";
      this.left = left;
      this.right = right;
      this.comparison = comparison;
    }

    public String getType() {
      return type;
    }

    public FieldBaseCompareExpression getLeft() {
      return left;
    }

    public FieldBaseCompareExpression getRight() {
      return right;
    }

    /**
     * @return Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN', 'LESS_EQUAL_THAN'
     */
    public String getComparison() {
      return comparison;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CompareSubsetCondition that = (CompareSubsetCondition) o;
      if (!Objects.equals(left, that.left)) return false;
      if (!Objects.equals(right, that.right)) return false;
      if (!Objects.equals(comparison, that.comparison)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(left, right, comparison);
    }
  }

  /**
   * builder class for CompareSubsetCondition
   */
  public static final class CompareSubsetConditionBuilder implements Builder<CompareSubsetCondition> {
    private FieldBaseCompareExpression left;

    private FieldBaseCompareExpression right;

    private String comparison;

    /**
     * @param left the left
     * @return this
     */
    public CompareSubsetConditionBuilder left(FieldBaseCompareExpression left) {
      this.left = left;
      return this;
    }

    /**
     * @param right the right
     * @return this
     */
    public CompareSubsetConditionBuilder right(FieldBaseCompareExpression right) {
      this.right = right;
      return this;
    }

    /**
     * @param comparison Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN', 'LESS_EQUAL_THAN'
     * @return this
     */
    public CompareSubsetConditionBuilder comparison(String comparison) {
      this.comparison = comparison;
      return this;
    }

    /**
     * build an instance of CompareSubsetCondition from the fields set in the builder
     */
    @Override
    public CompareSubsetCondition build() {
      CompareSubsetCondition __result = new CompareSubsetCondition(left, right, comparison);
      return __result;
    }
  }

  /**
   * model class for LineageJobFieldEntry
   */
  @JsonDeserialize(
      as = LineageJobFieldEntry.class
  )
  @JsonPropertyOrder("inputs")
  public static final class LineageJobFieldEntry {
    private final List<LineageJobInput> inputs;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param inputs Source entities and/or fields that feed into this target field.
     */
    @JsonCreator
    private LineageJobFieldEntry(@JsonProperty("inputs") List<LineageJobInput> inputs) {
      this.inputs = inputs;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return Source entities and/or fields that feed into this target field.
     */
    public List<LineageJobInput> getInputs() {
      return inputs;
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
      LineageJobFieldEntry that = (LineageJobFieldEntry) o;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(inputs, additionalProperties);
    }
  }

  /**
   * builder class for LineageJobFieldEntry
   */
  public static final class LineageJobFieldEntryBuilder implements Builder<LineageJobFieldEntry> {
    private List<LineageJobInput> inputs;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param inputs Source entities and/or fields that feed into this target field.
     * @return this
     */
    public LineageJobFieldEntryBuilder inputs(List<LineageJobInput> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageJobFieldEntryBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageJobFieldEntry from the fields set in the builder
     */
    @Override
    public LineageJobFieldEntry build() {
      LineageJobFieldEntry __result = new LineageJobFieldEntry(inputs);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for TagsRunFacet
   */
  @JsonDeserialize(
      as = TagsRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "tags"
  })
  public static final class TagsRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final List<TagsRunFacetFields> tags;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param tags The tags applied to the run facet
     */
    @JsonCreator
    private TagsRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("tags") List<TagsRunFacetFields> tags) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json#/$defs/TagsRunFacet");
      this.tags = tags;
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
     * @return The tags applied to the run facet
     */
    public List<TagsRunFacetFields> getTags() {
      return tags;
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
      TagsRunFacet that = (TagsRunFacet) o;
      if (!Objects.equals(tags, that.tags)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tags, additionalProperties);
    }
  }

  /**
   * builder class for TagsRunFacet
   */
  public final class TagsRunFacetBuilder implements Builder<TagsRunFacet> {
    private List<TagsRunFacetFields> tags;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param tags The tags applied to the run facet
     * @return this
     */
    public TagsRunFacetBuilder tags(List<TagsRunFacetFields> tags) {
      this.tags = tags;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public TagsRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of TagsRunFacet from the fields set in the builder
     */
    @Override
    public TagsRunFacet build() {
      TagsRunFacet __result = new TagsRunFacet(OpenLineage.this.producer, tags);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for JobDependency
   */
  @JsonDeserialize(
      as = JobDependency.class
  )
  @JsonPropertyOrder({
      "job",
      "run",
      "dependency_type",
      "sequence_trigger_rule",
      "status_trigger_rule"
  })
  public static final class JobDependency {
    private final JobIdentifier job;

    private final RunIdentifier run;

    private final String dependency_type;

    private final String sequence_trigger_rule;

    private final String status_trigger_rule;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param job the job
     * @param run the run
     * @param dependency_type Used to describe whether the upstream job directly triggers the downstream job, or whether the dependency is implicit (e.g. time-based).
     * @param sequence_trigger_rule Used to describe the exact sequence condition on which the downstream job can be executed (FINISH_TO_START - downstream job can start when upstream finished; FINISH_TO_FINISH - job executions can overlap, but need to finish in specified order; START_TO_START - jobs need to start at the same time in parallel).
     * @param status_trigger_rule Used to describe if the downstream job should be run based on the status of the upstream job.
     */
    @JsonCreator
    private JobDependency(@JsonProperty("job") JobIdentifier job,
        @JsonProperty("run") RunIdentifier run,
        @JsonProperty("dependency_type") String dependency_type,
        @JsonProperty("sequence_trigger_rule") String sequence_trigger_rule,
        @JsonProperty("status_trigger_rule") String status_trigger_rule) {
      this.job = job;
      this.run = run;
      this.dependency_type = dependency_type;
      this.sequence_trigger_rule = sequence_trigger_rule;
      this.status_trigger_rule = status_trigger_rule;
      this.additionalProperties = new LinkedHashMap<>();
    }

    public JobIdentifier getJob() {
      return job;
    }

    public RunIdentifier getRun() {
      return run;
    }

    /**
     * @return Used to describe whether the upstream job directly triggers the downstream job, or whether the dependency is implicit (e.g. time-based).
     */
    public String getDependency_type() {
      return dependency_type;
    }

    /**
     * @return Used to describe the exact sequence condition on which the downstream job can be executed (FINISH_TO_START - downstream job can start when upstream finished; FINISH_TO_FINISH - job executions can overlap, but need to finish in specified order; START_TO_START - jobs need to start at the same time in parallel).
     */
    public String getSequence_trigger_rule() {
      return sequence_trigger_rule;
    }

    /**
     * @return Used to describe if the downstream job should be run based on the status of the upstream job.
     */
    public String getStatus_trigger_rule() {
      return status_trigger_rule;
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
      JobDependency that = (JobDependency) o;
      if (!Objects.equals(job, that.job)) return false;
      if (!Objects.equals(run, that.run)) return false;
      if (!Objects.equals(dependency_type, that.dependency_type)) return false;
      if (!Objects.equals(sequence_trigger_rule, that.sequence_trigger_rule)) return false;
      if (!Objects.equals(status_trigger_rule, that.status_trigger_rule)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(job, run, dependency_type, sequence_trigger_rule, status_trigger_rule, additionalProperties);
    }
  }

  /**
   * builder class for JobDependency
   */
  public static final class JobDependencyBuilder implements Builder<JobDependency> {
    private JobIdentifier job;

    private RunIdentifier run;

    private String dependency_type;

    private String sequence_trigger_rule;

    private String status_trigger_rule;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param job the job
     * @return this
     */
    public JobDependencyBuilder job(JobIdentifier job) {
      this.job = job;
      return this;
    }

    /**
     * @param run the run
     * @return this
     */
    public JobDependencyBuilder run(RunIdentifier run) {
      this.run = run;
      return this;
    }

    /**
     * @param dependency_type Used to describe whether the upstream job directly triggers the downstream job, or whether the dependency is implicit (e.g. time-based).
     * @return this
     */
    public JobDependencyBuilder dependency_type(String dependency_type) {
      this.dependency_type = dependency_type;
      return this;
    }

    /**
     * @param sequence_trigger_rule Used to describe the exact sequence condition on which the downstream job can be executed (FINISH_TO_START - downstream job can start when upstream finished; FINISH_TO_FINISH - job executions can overlap, but need to finish in specified order; START_TO_START - jobs need to start at the same time in parallel).
     * @return this
     */
    public JobDependencyBuilder sequence_trigger_rule(String sequence_trigger_rule) {
      this.sequence_trigger_rule = sequence_trigger_rule;
      return this;
    }

    /**
     * @param status_trigger_rule Used to describe if the downstream job should be run based on the status of the upstream job.
     * @return this
     */
    public JobDependencyBuilder status_trigger_rule(String status_trigger_rule) {
      this.status_trigger_rule = status_trigger_rule;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public JobDependencyBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of JobDependency from the fields set in the builder
     */
    @Override
    public JobDependency build() {
      JobDependency __result = new JobDependency(job, run, dependency_type, sequence_trigger_rule, status_trigger_rule);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LifecycleStateChangeDatasetFacet
   */
  @JsonDeserialize(
      as = LifecycleStateChangeDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "lifecycleStateChange",
      "previousIdentifier"
  })
  public static final class LifecycleStateChangeDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange;

    private final LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param lifecycleStateChange The lifecycle state change.
     * @param previousIdentifier Previous name of the dataset in case of renaming it.
     */
    @JsonCreator
    private LifecycleStateChangeDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("lifecycleStateChange") LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange,
        @JsonProperty("previousIdentifier") LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet");
      this._deleted = null;
      this.lifecycleStateChange = lifecycleStateChange;
      this.previousIdentifier = previousIdentifier;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The lifecycle state change.
     */
    public LifecycleStateChangeDatasetFacet.LifecycleStateChange getLifecycleStateChange() {
      return lifecycleStateChange;
    }

    /**
     * @return Previous name of the dataset in case of renaming it.
     */
    public LifecycleStateChangeDatasetFacetPreviousIdentifier getPreviousIdentifier() {
      return previousIdentifier;
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
      LifecycleStateChangeDatasetFacet that = (LifecycleStateChangeDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(lifecycleStateChange, that.lifecycleStateChange)) return false;
      if (!Objects.equals(previousIdentifier, that.previousIdentifier)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, lifecycleStateChange, previousIdentifier, additionalProperties);
    }

    public enum LifecycleStateChange {
      ALTER,

      CREATE,

      DROP,

      OVERWRITE,

      RENAME,

      TRUNCATE
    }
  }

  /**
   * builder class for LifecycleStateChangeDatasetFacet
   */
  public final class LifecycleStateChangeDatasetFacetBuilder implements Builder<LifecycleStateChangeDatasetFacet> {
    private LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange;

    private LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param lifecycleStateChange The lifecycle state change.
     * @return this
     */
    public LifecycleStateChangeDatasetFacetBuilder lifecycleStateChange(
        LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange) {
      this.lifecycleStateChange = lifecycleStateChange;
      return this;
    }

    /**
     * @param previousIdentifier Previous name of the dataset in case of renaming it.
     * @return this
     */
    public LifecycleStateChangeDatasetFacetBuilder previousIdentifier(
        LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier) {
      this.previousIdentifier = previousIdentifier;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LifecycleStateChangeDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LifecycleStateChangeDatasetFacet from the fields set in the builder
     */
    @Override
    public LifecycleStateChangeDatasetFacet build() {
      LifecycleStateChangeDatasetFacet __result = new LifecycleStateChangeDatasetFacet(OpenLineage.this.producer, lifecycleStateChange, previousIdentifier);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for RunEvent
   */
  @JsonDeserialize(
      as = RunEvent.class
  )
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

    /**
     * @param eventTime the time the event occurred at
     * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param eventType the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete
     * @param run the run
     * @param job the job
     * @param inputs The set of **input** datasets.
     * @param outputs The set of **output** datasets.
     */
    @JsonCreator
    private RunEvent(@JsonProperty("eventTime") ZonedDateTime eventTime,
        @JsonProperty("producer") URI producer,
        @JsonProperty("eventType") RunEvent.EventType eventType, @JsonProperty("run") Run run,
        @JsonProperty("job") Job job, @JsonProperty("inputs") List<InputDataset> inputs,
        @JsonProperty("outputs") List<OutputDataset> outputs) {
      this.eventTime = eventTime;
      this.producer = producer;
      this.schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent");
      this.eventType = eventType;
      this.run = run;
      this.job = job;
      this.inputs = inputs;
      this.outputs = outputs;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RunEvent that = (RunEvent) o;
      if (!Objects.equals(eventTime, that.eventTime)) return false;
      if (!Objects.equals(eventType, that.eventType)) return false;
      if (!Objects.equals(run, that.run)) return false;
      if (!Objects.equals(job, that.job)) return false;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(outputs, that.outputs)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(eventTime, eventType, run, job, inputs, outputs);
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
   * builder class for RunEvent
   */
  public final class RunEventBuilder implements Builder<RunEvent> {
    private ZonedDateTime eventTime;

    private RunEvent.EventType eventType;

    private Run run;

    private Job job;

    private List<InputDataset> inputs;

    private List<OutputDataset> outputs;

    /**
     * @param eventTime the time the event occurred at
     * @return this
     */
    public RunEventBuilder eventTime(ZonedDateTime eventTime) {
      this.eventTime = eventTime;
      return this;
    }

    /**
     * @param eventType the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete
     * @return this
     */
    public RunEventBuilder eventType(RunEvent.EventType eventType) {
      this.eventType = eventType;
      return this;
    }

    /**
     * @param run the run
     * @return this
     */
    public RunEventBuilder run(Run run) {
      this.run = run;
      return this;
    }

    /**
     * @param job the job
     * @return this
     */
    public RunEventBuilder job(Job job) {
      this.job = job;
      return this;
    }

    /**
     * @param inputs The set of **input** datasets.
     * @return this
     */
    public RunEventBuilder inputs(List<InputDataset> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * @param outputs The set of **output** datasets.
     * @return this
     */
    public RunEventBuilder outputs(List<OutputDataset> outputs) {
      this.outputs = outputs;
      return this;
    }

    /**
     * build an instance of RunEvent from the fields set in the builder
     */
    @Override
    public RunEvent build() {
      RunEvent __result = new RunEvent(eventTime, OpenLineage.this.producer, eventType, run, job, inputs, outputs);
      return __result;
    }
  }

  /**
   * model class for TagsDatasetFacet
   */
  @JsonDeserialize(
      as = TagsDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "tags"
  })
  public static final class TagsDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<TagsDatasetFacetFields> tags;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param tags The tags applied to the dataset facet
     */
    @JsonCreator
    private TagsDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("tags") List<TagsDatasetFacetFields> tags) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json#/$defs/TagsDatasetFacet");
      this._deleted = null;
      this.tags = tags;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The tags applied to the dataset facet
     */
    public List<TagsDatasetFacetFields> getTags() {
      return tags;
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
      TagsDatasetFacet that = (TagsDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(tags, that.tags)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, tags, additionalProperties);
    }
  }

  /**
   * builder class for TagsDatasetFacet
   */
  public final class TagsDatasetFacetBuilder implements Builder<TagsDatasetFacet> {
    private List<TagsDatasetFacetFields> tags;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param tags The tags applied to the dataset facet
     * @return this
     */
    public TagsDatasetFacetBuilder tags(List<TagsDatasetFacetFields> tags) {
      this.tags = tags;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public TagsDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of TagsDatasetFacet from the fields set in the builder
     */
    @Override
    public TagsDatasetFacet build() {
      TagsDatasetFacet __result = new TagsDatasetFacet(OpenLineage.this.producer, tags);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for CatalogDatasetFacetCatalogProperties
   */
  @JsonDeserialize(
      as = CatalogDatasetFacetCatalogProperties.class
  )
  @JsonPropertyOrder
  public static final class CatalogDatasetFacetCatalogProperties {
    @JsonAnySetter
    private final Map<String, String> additionalProperties;

    @JsonCreator
    private CatalogDatasetFacetCatalogProperties() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, String> getAdditionalProperties() {
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
      CatalogDatasetFacetCatalogProperties that = (CatalogDatasetFacetCatalogProperties) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for CatalogDatasetFacetCatalogProperties
   */
  public static final class CatalogDatasetFacetCatalogPropertiesBuilder implements Builder<CatalogDatasetFacetCatalogProperties> {
    private final Map<String, String> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public CatalogDatasetFacetCatalogPropertiesBuilder put(String key, String value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of CatalogDatasetFacetCatalogProperties from the fields set in the builder
     */
    @Override
    public CatalogDatasetFacetCatalogProperties build() {
      CatalogDatasetFacetCatalogProperties __result = new CatalogDatasetFacetCatalogProperties();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ExecutionParameter
   */
  @JsonDeserialize(
      as = ExecutionParameter.class
  )
  @JsonPropertyOrder({
      "key",
      "name",
      "description",
      "value"
  })
  public static final class ExecutionParameter {
    private final String key;

    private final String name;

    private final String description;

    private final String value;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param key Unique identifier of the property.
     * @param name Human-readable name of the property.
     * @param description Human-readable description of the property.
     * @param value Value of the property.
     */
    @JsonCreator
    private ExecutionParameter(@JsonProperty("key") String key, @JsonProperty("name") String name,
        @JsonProperty("description") String description, @JsonProperty("value") String value) {
      this.key = key;
      this.name = name;
      this.description = description;
      this.value = value;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return Unique identifier of the property.
     */
    public String getKey() {
      return key;
    }

    /**
     * @return Human-readable name of the property.
     */
    public String getName() {
      return name;
    }

    /**
     * @return Human-readable description of the property.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return Value of the property.
     */
    public String getValue() {
      return value;
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
      ExecutionParameter that = (ExecutionParameter) o;
      if (!Objects.equals(key, that.key)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(value, that.value)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, name, description, value, additionalProperties);
    }
  }

  /**
   * builder class for ExecutionParameter
   */
  public static final class ExecutionParameterBuilder implements Builder<ExecutionParameter> {
    private String key;

    private String name;

    private String description;

    private String value;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param key Unique identifier of the property.
     * @return this
     */
    public ExecutionParameterBuilder key(String key) {
      this.key = key;
      return this;
    }

    /**
     * @param name Human-readable name of the property.
     * @return this
     */
    public ExecutionParameterBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param description Human-readable description of the property.
     * @return this
     */
    public ExecutionParameterBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param value Value of the property.
     * @return this
     */
    public ExecutionParameterBuilder value(String value) {
      this.value = value;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ExecutionParameterBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ExecutionParameter from the fields set in the builder
     */
    @Override
    public ExecutionParameter build() {
      ExecutionParameter __result = new ExecutionParameter(key, name, description, value);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for TestRunFacet
   */
  @JsonDeserialize(
      as = TestRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "tests"
  })
  public static final class TestRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final List<TestExecution> tests;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param tests List of test executions and their results.
     */
    @JsonCreator
    private TestRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("tests") List<TestExecution> tests) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/TestRunFacet.json#/$defs/TestRunFacet");
      this.tests = tests;
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
     * @return List of test executions and their results.
     */
    public List<TestExecution> getTests() {
      return tests;
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
      TestRunFacet that = (TestRunFacet) o;
      if (!Objects.equals(tests, that.tests)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tests, additionalProperties);
    }
  }

  /**
   * builder class for TestRunFacet
   */
  public final class TestRunFacetBuilder implements Builder<TestRunFacet> {
    private List<TestExecution> tests;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param tests List of test executions and their results.
     * @return this
     */
    public TestRunFacetBuilder tests(List<TestExecution> tests) {
      this.tests = tests;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public TestRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of TestRunFacet from the fields set in the builder
     */
    @Override
    public TestRunFacet build() {
      TestRunFacet __result = new TestRunFacet(OpenLineage.this.producer, tests);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for RootRun
   */
  @JsonDeserialize(
      as = RootRun.class
  )
  @JsonPropertyOrder("runId")
  public static final class RootRun {
    private final UUID runId;

    /**
     * @param runId The globally unique ID of the root run associated with the root job.
     */
    @JsonCreator
    private RootRun(@JsonProperty("runId") UUID runId) {
      this.runId = runId;
    }

    /**
     * @return The globally unique ID of the root run associated with the root job.
     */
    public UUID getRunId() {
      return runId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RootRun that = (RootRun) o;
      if (!Objects.equals(runId, that.runId)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(runId);
    }
  }

  /**
   * builder class for RootRun
   */
  public static final class RootRunBuilder implements Builder<RootRun> {
    private UUID runId;

    /**
     * @param runId The globally unique ID of the root run associated with the root job.
     * @return this
     */
    public RootRunBuilder runId(UUID runId) {
      this.runId = runId;
      return this;
    }

    /**
     * build an instance of RootRun from the fields set in the builder
     */
    @Override
    public RootRun build() {
      RootRun __result = new RootRun(runId);
      return __result;
    }
  }

  public static class DefaultRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    @JsonCreator
    public DefaultRunFacet(@JsonProperty("_producer") URI _producer) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet");
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
  }

  /**
   * Interface for RunFacet
   */
  @JsonDeserialize(
      as = DefaultRunFacet.class
  )
  public interface RunFacet {
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
   * model class for HierarchyDatasetFacetLevel
   */
  @JsonDeserialize(
      as = HierarchyDatasetFacetLevel.class
  )
  @JsonPropertyOrder({
      "type",
      "name"
  })
  public static final class HierarchyDatasetFacetLevel {
    private final String type;

    private final String name;

    /**
     * @param type Dataset hierarchy level type
     * @param name Dataset hierarchy level name
     */
    @JsonCreator
    private HierarchyDatasetFacetLevel(@JsonProperty("type") String type,
        @JsonProperty("name") String name) {
      this.type = type;
      this.name = name;
    }

    /**
     * @return Dataset hierarchy level type
     */
    public String getType() {
      return type;
    }

    /**
     * @return Dataset hierarchy level name
     */
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      HierarchyDatasetFacetLevel that = (HierarchyDatasetFacetLevel) o;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(name, that.name)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, name);
    }
  }

  /**
   * builder class for HierarchyDatasetFacetLevel
   */
  public static final class HierarchyDatasetFacetLevelBuilder implements Builder<HierarchyDatasetFacetLevel> {
    private String type;

    private String name;

    /**
     * @param type Dataset hierarchy level type
     * @return this
     */
    public HierarchyDatasetFacetLevelBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param name Dataset hierarchy level name
     * @return this
     */
    public HierarchyDatasetFacetLevelBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * build an instance of HierarchyDatasetFacetLevel from the fields set in the builder
     */
    @Override
    public HierarchyDatasetFacetLevel build() {
      HierarchyDatasetFacetLevel __result = new HierarchyDatasetFacetLevel(type, name);
      return __result;
    }
  }

  /**
   * model class for OwnershipJobFacetOwners
   */
  @JsonDeserialize(
      as = OwnershipJobFacetOwners.class
  )
  @JsonPropertyOrder({
      "name",
      "type"
  })
  public static final class OwnershipJobFacetOwners {
    private final String name;

    private final String type;

    /**
     * @param name the identifier of the owner of the Job. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
     * @param type The type of ownership (optional)
     */
    @JsonCreator
    private OwnershipJobFacetOwners(@JsonProperty("name") String name,
        @JsonProperty("type") String type) {
      this.name = name;
      this.type = type;
    }

    /**
     * @return the identifier of the owner of the Job. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of ownership (optional)
     */
    public String getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OwnershipJobFacetOwners that = (OwnershipJobFacetOwners) o;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }
  }

  /**
   * builder class for OwnershipJobFacetOwners
   */
  public static final class OwnershipJobFacetOwnersBuilder implements Builder<OwnershipJobFacetOwners> {
    private String name;

    private String type;

    /**
     * @param name the identifier of the owner of the Job. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
     * @return this
     */
    public OwnershipJobFacetOwnersBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of ownership (optional)
     * @return this
     */
    public OwnershipJobFacetOwnersBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * build an instance of OwnershipJobFacetOwners from the fields set in the builder
     */
    @Override
    public OwnershipJobFacetOwners build() {
      OwnershipJobFacetOwners __result = new OwnershipJobFacetOwners(name, type);
      return __result;
    }
  }

  /**
   * model class for GcpLineageJobFacet
   */
  @JsonDeserialize(
      as = GcpLineageJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "displayName",
      "origin"
  })
  public static final class GcpLineageJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String displayName;

    private final GcpLineageJobFacetOrigin origin;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param displayName The name of the job to be used on UI
     * @param origin the origin
     */
    @JsonCreator
    private GcpLineageJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("displayName") String displayName,
        @JsonProperty("origin") GcpLineageJobFacetOrigin origin) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/GcpLineageJobFacet.json#/$defs/GcpLineageJobFacet");
      this._deleted = null;
      this.displayName = displayName;
      this.origin = origin;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The name of the job to be used on UI
     */
    public String getDisplayName() {
      return displayName;
    }

    public GcpLineageJobFacetOrigin getOrigin() {
      return origin;
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
      GcpLineageJobFacet that = (GcpLineageJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(displayName, that.displayName)) return false;
      if (!Objects.equals(origin, that.origin)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, displayName, origin, additionalProperties);
    }
  }

  /**
   * builder class for GcpLineageJobFacet
   */
  public final class GcpLineageJobFacetBuilder implements Builder<GcpLineageJobFacet> {
    private String displayName;

    private GcpLineageJobFacetOrigin origin;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param displayName The name of the job to be used on UI
     * @return this
     */
    public GcpLineageJobFacetBuilder displayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    /**
     * @param origin the origin
     * @return this
     */
    public GcpLineageJobFacetBuilder origin(GcpLineageJobFacetOrigin origin) {
      this.origin = origin;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public GcpLineageJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of GcpLineageJobFacet from the fields set in the builder
     */
    @Override
    public GcpLineageJobFacet build() {
      GcpLineageJobFacet __result = new GcpLineageJobFacet(OpenLineage.this.producer, displayName, origin);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for JobEvent
   */
  @JsonDeserialize(
      as = JobEvent.class
  )
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

    /**
     * @param eventTime the time the event occurred at
     * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param job the job
     * @param inputs The set of **input** datasets.
     * @param outputs The set of **output** datasets.
     */
    @JsonCreator
    private JobEvent(@JsonProperty("eventTime") ZonedDateTime eventTime,
        @JsonProperty("producer") URI producer, @JsonProperty("job") Job job,
        @JsonProperty("inputs") List<InputDataset> inputs,
        @JsonProperty("outputs") List<OutputDataset> outputs) {
      this.eventTime = eventTime;
      this.producer = producer;
      this.schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent");
      this.job = job;
      this.inputs = inputs;
      this.outputs = outputs;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JobEvent that = (JobEvent) o;
      if (!Objects.equals(eventTime, that.eventTime)) return false;
      if (!Objects.equals(job, that.job)) return false;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(outputs, that.outputs)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(eventTime, job, inputs, outputs);
    }
  }

  /**
   * builder class for JobEvent
   */
  public final class JobEventBuilder implements Builder<JobEvent> {
    private ZonedDateTime eventTime;

    private Job job;

    private List<InputDataset> inputs;

    private List<OutputDataset> outputs;

    /**
     * @param eventTime the time the event occurred at
     * @return this
     */
    public JobEventBuilder eventTime(ZonedDateTime eventTime) {
      this.eventTime = eventTime;
      return this;
    }

    /**
     * @param job the job
     * @return this
     */
    public JobEventBuilder job(Job job) {
      this.job = job;
      return this;
    }

    /**
     * @param inputs The set of **input** datasets.
     * @return this
     */
    public JobEventBuilder inputs(List<InputDataset> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * @param outputs The set of **output** datasets.
     * @return this
     */
    public JobEventBuilder outputs(List<OutputDataset> outputs) {
      this.outputs = outputs;
      return this;
    }

    /**
     * build an instance of JobEvent from the fields set in the builder
     */
    @Override
    public JobEvent build() {
      JobEvent __result = new JobEvent(eventTime, OpenLineage.this.producer, job, inputs, outputs);
      return __result;
    }
  }

  /**
   * model class for ExtractionErrorRunFacet
   */
  @JsonDeserialize(
      as = ExtractionErrorRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "totalTasks",
      "failedTasks",
      "errors"
  })
  public static final class ExtractionErrorRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Long totalTasks;

    private final Long failedTasks;

    private final List<ExtractionErrorRunFacetErrors> errors;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param totalTasks The number of distinguishable tasks in a run that were processed by OpenLineage, whether successfully or not. Those could be, for example, distinct SQL statements.
     * @param failedTasks The number of distinguishable tasks in a run that were processed not successfully by OpenLineage. Those could be, for example, distinct SQL statements.
     * @param errors the errors
     */
    @JsonCreator
    private ExtractionErrorRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("totalTasks") Long totalTasks, @JsonProperty("failedTasks") Long failedTasks,
        @JsonProperty("errors") List<ExtractionErrorRunFacetErrors> errors) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-2/ExtractionErrorRunFacet.json#/$defs/ExtractionErrorRunFacet");
      this.totalTasks = totalTasks;
      this.failedTasks = failedTasks;
      this.errors = errors;
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
     * @return The number of distinguishable tasks in a run that were processed by OpenLineage, whether successfully or not. Those could be, for example, distinct SQL statements.
     */
    public Long getTotalTasks() {
      return totalTasks;
    }

    /**
     * @return The number of distinguishable tasks in a run that were processed not successfully by OpenLineage. Those could be, for example, distinct SQL statements.
     */
    public Long getFailedTasks() {
      return failedTasks;
    }

    public List<ExtractionErrorRunFacetErrors> getErrors() {
      return errors;
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
      ExtractionErrorRunFacet that = (ExtractionErrorRunFacet) o;
      if (!Objects.equals(totalTasks, that.totalTasks)) return false;
      if (!Objects.equals(failedTasks, that.failedTasks)) return false;
      if (!Objects.equals(errors, that.errors)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(totalTasks, failedTasks, errors, additionalProperties);
    }
  }

  /**
   * builder class for ExtractionErrorRunFacet
   */
  public final class ExtractionErrorRunFacetBuilder implements Builder<ExtractionErrorRunFacet> {
    private Long totalTasks;

    private Long failedTasks;

    private List<ExtractionErrorRunFacetErrors> errors;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param totalTasks The number of distinguishable tasks in a run that were processed by OpenLineage, whether successfully or not. Those could be, for example, distinct SQL statements.
     * @return this
     */
    public ExtractionErrorRunFacetBuilder totalTasks(Long totalTasks) {
      this.totalTasks = totalTasks;
      return this;
    }

    /**
     * @param failedTasks The number of distinguishable tasks in a run that were processed not successfully by OpenLineage. Those could be, for example, distinct SQL statements.
     * @return this
     */
    public ExtractionErrorRunFacetBuilder failedTasks(Long failedTasks) {
      this.failedTasks = failedTasks;
      return this;
    }

    /**
     * @param errors the errors
     * @return this
     */
    public ExtractionErrorRunFacetBuilder errors(List<ExtractionErrorRunFacetErrors> errors) {
      this.errors = errors;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ExtractionErrorRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ExtractionErrorRunFacet from the fields set in the builder
     */
    @Override
    public ExtractionErrorRunFacet build() {
      ExtractionErrorRunFacet __result = new ExtractionErrorRunFacet(OpenLineage.this.producer, totalTasks, failedTasks, errors);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for OwnershipDatasetFacetOwners
   */
  @JsonDeserialize(
      as = OwnershipDatasetFacetOwners.class
  )
  @JsonPropertyOrder({
      "name",
      "type"
  })
  public static final class OwnershipDatasetFacetOwners {
    private final String name;

    private final String type;

    /**
     * @param name the identifier of the owner of the Dataset. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
     * @param type The type of ownership (optional)
     */
    @JsonCreator
    private OwnershipDatasetFacetOwners(@JsonProperty("name") String name,
        @JsonProperty("type") String type) {
      this.name = name;
      this.type = type;
    }

    /**
     * @return the identifier of the owner of the Dataset. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of ownership (optional)
     */
    public String getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OwnershipDatasetFacetOwners that = (OwnershipDatasetFacetOwners) o;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }
  }

  /**
   * builder class for OwnershipDatasetFacetOwners
   */
  public static final class OwnershipDatasetFacetOwnersBuilder implements Builder<OwnershipDatasetFacetOwners> {
    private String name;

    private String type;

    /**
     * @param name the identifier of the owner of the Dataset. It is recommended to define this as a URN. For example application:foo, user:jdoe, team:data
     * @return this
     */
    public OwnershipDatasetFacetOwnersBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of ownership (optional)
     * @return this
     */
    public OwnershipDatasetFacetOwnersBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * build an instance of OwnershipDatasetFacetOwners from the fields set in the builder
     */
    @Override
    public OwnershipDatasetFacetOwners build() {
      OwnershipDatasetFacetOwners __result = new OwnershipDatasetFacetOwners(name, type);
      return __result;
    }
  }

  /**
   * model class for LineageJobFacet
   */
  @JsonDeserialize(
      as = LineageJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "lineage"
  })
  public static final class LineageJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<LineageJobEntry> lineage;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param lineage Lineage entries describing data flow declared for this job definition. Each entry identifies a target entity and the sources that feed into it.
     */
    @JsonCreator
    private LineageJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("lineage") List<LineageJobEntry> lineage) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/LineageJobFacet.json#/$defs/LineageJobFacet");
      this._deleted = null;
      this.lineage = lineage;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Lineage entries describing data flow declared for this job definition. Each entry identifies a target entity and the sources that feed into it.
     */
    public List<LineageJobEntry> getLineage() {
      return lineage;
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
      LineageJobFacet that = (LineageJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(lineage, that.lineage)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, lineage, additionalProperties);
    }
  }

  /**
   * builder class for LineageJobFacet
   */
  public final class LineageJobFacetBuilder implements Builder<LineageJobFacet> {
    private List<LineageJobEntry> lineage;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param lineage Lineage entries describing data flow declared for this job definition. Each entry identifies a target entity and the sources that feed into it.
     * @return this
     */
    public LineageJobFacetBuilder lineage(List<LineageJobEntry> lineage) {
      this.lineage = lineage;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageJobFacet from the fields set in the builder
     */
    @Override
    public LineageJobFacet build() {
      LineageJobFacet __result = new LineageJobFacet(OpenLineage.this.producer, lineage);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsInputDatasetFacetColumnMetrics
   */
  @JsonDeserialize(
      as = DataQualityMetricsInputDatasetFacetColumnMetrics.class
  )
  @JsonPropertyOrder
  public static final class DataQualityMetricsInputDatasetFacetColumnMetrics {
    @JsonAnySetter
    private final Map<String, DataQualityMetricsInputDatasetFacetColumnMetricsAdditional> additionalProperties;

    @JsonCreator
    private DataQualityMetricsInputDatasetFacetColumnMetrics() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, DataQualityMetricsInputDatasetFacetColumnMetricsAdditional> getAdditionalProperties(
        ) {
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
      DataQualityMetricsInputDatasetFacetColumnMetrics that = (DataQualityMetricsInputDatasetFacetColumnMetrics) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for DataQualityMetricsInputDatasetFacetColumnMetrics
   */
  public static final class DataQualityMetricsInputDatasetFacetColumnMetricsBuilder implements Builder<DataQualityMetricsInputDatasetFacetColumnMetrics> {
    private final Map<String, DataQualityMetricsInputDatasetFacetColumnMetricsAdditional> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DataQualityMetricsInputDatasetFacetColumnMetricsBuilder put(String key,
        DataQualityMetricsInputDatasetFacetColumnMetricsAdditional value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DataQualityMetricsInputDatasetFacetColumnMetrics from the fields set in the builder
     */
    @Override
    public DataQualityMetricsInputDatasetFacetColumnMetrics build() {
      DataQualityMetricsInputDatasetFacetColumnMetrics __result = new DataQualityMetricsInputDatasetFacetColumnMetrics();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageEntryFields
   */
  @JsonDeserialize(
      as = LineageEntryFields.class
  )
  @JsonPropertyOrder
  public static final class LineageEntryFields {
    @JsonAnySetter
    private final Map<String, LineageFieldEntry> additionalProperties;

    @JsonCreator
    private LineageEntryFields() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, LineageFieldEntry> getAdditionalProperties() {
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
      LineageEntryFields that = (LineageEntryFields) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for LineageEntryFields
   */
  public static final class LineageEntryFieldsBuilder implements Builder<LineageEntryFields> {
    private final Map<String, LineageFieldEntry> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageEntryFieldsBuilder put(String key, LineageFieldEntry value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageEntryFields from the fields set in the builder
     */
    @Override
    public LineageEntryFields build() {
      LineageEntryFields __result = new LineageEntryFields();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageDatasetFieldEntry
   */
  @JsonDeserialize(
      as = LineageDatasetFieldEntry.class
  )
  @JsonPropertyOrder("inputs")
  public static final class LineageDatasetFieldEntry {
    private final List<LineageDatasetInput> inputs;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param inputs Source entities and/or fields that feed into this target field.
     */
    @JsonCreator
    private LineageDatasetFieldEntry(@JsonProperty("inputs") List<LineageDatasetInput> inputs) {
      this.inputs = inputs;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return Source entities and/or fields that feed into this target field.
     */
    public List<LineageDatasetInput> getInputs() {
      return inputs;
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
      LineageDatasetFieldEntry that = (LineageDatasetFieldEntry) o;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(inputs, additionalProperties);
    }
  }

  /**
   * builder class for LineageDatasetFieldEntry
   */
  public static final class LineageDatasetFieldEntryBuilder implements Builder<LineageDatasetFieldEntry> {
    private List<LineageDatasetInput> inputs;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param inputs Source entities and/or fields that feed into this target field.
     * @return this
     */
    public LineageDatasetFieldEntryBuilder inputs(List<LineageDatasetInput> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageDatasetFieldEntryBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageDatasetFieldEntry from the fields set in the builder
     */
    @Override
    public LineageDatasetFieldEntry build() {
      LineageDatasetFieldEntry __result = new LineageDatasetFieldEntry(inputs);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for InputField
   */
  @JsonDeserialize(
      as = InputField.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "field",
      "transformations"
  })
  public static final class InputField {
    private final String namespace;

    private final String name;

    private final String field;

    private final List<InputFieldTransformations> transformations;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The input dataset namespace
     * @param name The input dataset name
     * @param field The input field
     * @param transformations the transformations
     */
    @JsonCreator
    private InputField(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("field") String field,
        @JsonProperty("transformations") List<InputFieldTransformations> transformations) {
      this.namespace = namespace;
      this.name = name;
      this.field = field;
      this.transformations = transformations;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The input dataset namespace
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The input dataset name
     */
    public String getName() {
      return name;
    }

    /**
     * @return The input field
     */
    public String getField() {
      return field;
    }

    public List<InputFieldTransformations> getTransformations() {
      return transformations;
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
      InputField that = (InputField) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(field, that.field)) return false;
      if (!Objects.equals(transformations, that.transformations)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, field, transformations, additionalProperties);
    }
  }

  /**
   * builder class for InputField
   */
  public static final class InputFieldBuilder implements Builder<InputField> {
    private String namespace;

    private String name;

    private String field;

    private List<InputFieldTransformations> transformations;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param namespace The input dataset namespace
     * @return this
     */
    public InputFieldBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The input dataset name
     * @return this
     */
    public InputFieldBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param field The input field
     * @return this
     */
    public InputFieldBuilder field(String field) {
      this.field = field;
      return this;
    }

    /**
     * @param transformations the transformations
     * @return this
     */
    public InputFieldBuilder transformations(List<InputFieldTransformations> transformations) {
      this.transformations = transformations;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public InputFieldBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of InputField from the fields set in the builder
     */
    @Override
    public InputField build() {
      InputField __result = new InputField(namespace, name, field, transformations);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for InputStatisticsInputDatasetFacet
   */
  @JsonDeserialize(
      as = InputStatisticsInputDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "rowCount",
      "size",
      "fileCount"
  })
  public static final class InputStatisticsInputDatasetFacet implements InputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Long rowCount;

    private final Long size;

    private final Long fileCount;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param rowCount The number of rows read
     * @param size The size in bytes read
     * @param fileCount The number of files read
     */
    @JsonCreator
    private InputStatisticsInputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("rowCount") Long rowCount, @JsonProperty("size") Long size,
        @JsonProperty("fileCount") Long fileCount) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/InputStatisticsInputDatasetFacet.json#/$defs/InputStatisticsInputDatasetFacet");
      this.rowCount = rowCount;
      this.size = size;
      this.fileCount = fileCount;
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
     * @return The number of rows read
     */
    public Long getRowCount() {
      return rowCount;
    }

    /**
     * @return The size in bytes read
     */
    public Long getSize() {
      return size;
    }

    /**
     * @return The number of files read
     */
    public Long getFileCount() {
      return fileCount;
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
      InputStatisticsInputDatasetFacet that = (InputStatisticsInputDatasetFacet) o;
      if (!Objects.equals(rowCount, that.rowCount)) return false;
      if (!Objects.equals(size, that.size)) return false;
      if (!Objects.equals(fileCount, that.fileCount)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(rowCount, size, fileCount, additionalProperties);
    }
  }

  /**
   * builder class for InputStatisticsInputDatasetFacet
   */
  public final class InputStatisticsInputDatasetFacetBuilder implements Builder<InputStatisticsInputDatasetFacet> {
    private Long rowCount;

    private Long size;

    private Long fileCount;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param rowCount The number of rows read
     * @return this
     */
    public InputStatisticsInputDatasetFacetBuilder rowCount(Long rowCount) {
      this.rowCount = rowCount;
      return this;
    }

    /**
     * @param size The size in bytes read
     * @return this
     */
    public InputStatisticsInputDatasetFacetBuilder size(Long size) {
      this.size = size;
      return this;
    }

    /**
     * @param fileCount The number of files read
     * @return this
     */
    public InputStatisticsInputDatasetFacetBuilder fileCount(Long fileCount) {
      this.fileCount = fileCount;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public InputStatisticsInputDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of InputStatisticsInputDatasetFacet from the fields set in the builder
     */
    @Override
    public InputStatisticsInputDatasetFacet build() {
      InputStatisticsInputDatasetFacet __result = new InputStatisticsInputDatasetFacet(OpenLineage.this.producer, rowCount, size, fileCount);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for JobFacets
   */
  @JsonDeserialize(
      as = JobFacets.class
  )
  @JsonPropertyOrder({
      "jobType",
      "sourceCode",
      "gcp_lineage",
      "sql",
      "lineage",
      "gcp_composer_job",
      "ownership",
      "sourceCodeLocation",
      "tags",
      "documentation"
  })
  public static final class JobFacets {
    private final JobTypeJobFacet jobType;

    private final SourceCodeJobFacet sourceCode;

    private final GcpLineageJobFacet gcp_lineage;

    private final SQLJobFacet sql;

    private final LineageJobFacet lineage;

    private final GcpComposerJobFacet gcp_composer_job;

    private final OwnershipJobFacet ownership;

    private final SourceCodeLocationJobFacet sourceCodeLocation;

    private final TagsJobFacet tags;

    private final DocumentationJobFacet documentation;

    @JsonAnySetter
    private final Map<String, JobFacet> additionalProperties;

    /**
     * @param jobType the jobType
     * @param sourceCode the sourceCode
     * @param gcp_lineage the gcp_lineage
     * @param sql the sql
     * @param lineage the lineage
     * @param gcp_composer_job the gcp_composer_job
     * @param ownership the ownership
     * @param sourceCodeLocation the sourceCodeLocation
     * @param tags the tags
     * @param documentation the documentation
     */
    @JsonCreator
    private JobFacets(@JsonProperty("jobType") JobTypeJobFacet jobType,
        @JsonProperty("sourceCode") SourceCodeJobFacet sourceCode,
        @JsonProperty("gcp_lineage") GcpLineageJobFacet gcp_lineage,
        @JsonProperty("sql") SQLJobFacet sql, @JsonProperty("lineage") LineageJobFacet lineage,
        @JsonProperty("gcp_composer_job") GcpComposerJobFacet gcp_composer_job,
        @JsonProperty("ownership") OwnershipJobFacet ownership,
        @JsonProperty("sourceCodeLocation") SourceCodeLocationJobFacet sourceCodeLocation,
        @JsonProperty("tags") TagsJobFacet tags,
        @JsonProperty("documentation") DocumentationJobFacet documentation) {
      this.jobType = jobType;
      this.sourceCode = sourceCode;
      this.gcp_lineage = gcp_lineage;
      this.sql = sql;
      this.lineage = lineage;
      this.gcp_composer_job = gcp_composer_job;
      this.ownership = ownership;
      this.sourceCodeLocation = sourceCodeLocation;
      this.tags = tags;
      this.documentation = documentation;
      this.additionalProperties = new LinkedHashMap<>();
    }

    public JobTypeJobFacet getJobType() {
      return jobType;
    }

    public SourceCodeJobFacet getSourceCode() {
      return sourceCode;
    }

    public GcpLineageJobFacet getGcp_lineage() {
      return gcp_lineage;
    }

    public SQLJobFacet getSql() {
      return sql;
    }

    public LineageJobFacet getLineage() {
      return lineage;
    }

    public GcpComposerJobFacet getGcp_composer_job() {
      return gcp_composer_job;
    }

    public OwnershipJobFacet getOwnership() {
      return ownership;
    }

    public SourceCodeLocationJobFacet getSourceCodeLocation() {
      return sourceCodeLocation;
    }

    public TagsJobFacet getTags() {
      return tags;
    }

    public DocumentationJobFacet getDocumentation() {
      return documentation;
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
      if (!Objects.equals(jobType, that.jobType)) return false;
      if (!Objects.equals(sourceCode, that.sourceCode)) return false;
      if (!Objects.equals(gcp_lineage, that.gcp_lineage)) return false;
      if (!Objects.equals(sql, that.sql)) return false;
      if (!Objects.equals(lineage, that.lineage)) return false;
      if (!Objects.equals(gcp_composer_job, that.gcp_composer_job)) return false;
      if (!Objects.equals(ownership, that.ownership)) return false;
      if (!Objects.equals(sourceCodeLocation, that.sourceCodeLocation)) return false;
      if (!Objects.equals(tags, that.tags)) return false;
      if (!Objects.equals(documentation, that.documentation)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobType, sourceCode, gcp_lineage, sql, lineage, gcp_composer_job, ownership, sourceCodeLocation, tags, documentation, additionalProperties);
    }
  }

  /**
   * builder class for JobFacets
   */
  public static final class JobFacetsBuilder implements Builder<JobFacets> {
    private JobTypeJobFacet jobType;

    private SourceCodeJobFacet sourceCode;

    private GcpLineageJobFacet gcp_lineage;

    private SQLJobFacet sql;

    private LineageJobFacet lineage;

    private GcpComposerJobFacet gcp_composer_job;

    private OwnershipJobFacet ownership;

    private SourceCodeLocationJobFacet sourceCodeLocation;

    private TagsJobFacet tags;

    private DocumentationJobFacet documentation;

    private final Map<String, JobFacet> additionalProperties = new LinkedHashMap<>();

    /**
     * @param jobType the jobType
     * @return this
     */
    public JobFacetsBuilder jobType(JobTypeJobFacet jobType) {
      this.jobType = jobType;
      return this;
    }

    /**
     * @param sourceCode the sourceCode
     * @return this
     */
    public JobFacetsBuilder sourceCode(SourceCodeJobFacet sourceCode) {
      this.sourceCode = sourceCode;
      return this;
    }

    /**
     * @param gcp_lineage the gcp_lineage
     * @return this
     */
    public JobFacetsBuilder gcp_lineage(GcpLineageJobFacet gcp_lineage) {
      this.gcp_lineage = gcp_lineage;
      return this;
    }

    /**
     * @param sql the sql
     * @return this
     */
    public JobFacetsBuilder sql(SQLJobFacet sql) {
      this.sql = sql;
      return this;
    }

    /**
     * @param lineage the lineage
     * @return this
     */
    public JobFacetsBuilder lineage(LineageJobFacet lineage) {
      this.lineage = lineage;
      return this;
    }

    /**
     * @param gcp_composer_job the gcp_composer_job
     * @return this
     */
    public JobFacetsBuilder gcp_composer_job(GcpComposerJobFacet gcp_composer_job) {
      this.gcp_composer_job = gcp_composer_job;
      return this;
    }

    /**
     * @param ownership the ownership
     * @return this
     */
    public JobFacetsBuilder ownership(OwnershipJobFacet ownership) {
      this.ownership = ownership;
      return this;
    }

    /**
     * @param sourceCodeLocation the sourceCodeLocation
     * @return this
     */
    public JobFacetsBuilder sourceCodeLocation(SourceCodeLocationJobFacet sourceCodeLocation) {
      this.sourceCodeLocation = sourceCodeLocation;
      return this;
    }

    /**
     * @param tags the tags
     * @return this
     */
    public JobFacetsBuilder tags(TagsJobFacet tags) {
      this.tags = tags;
      return this;
    }

    /**
     * @param documentation the documentation
     * @return this
     */
    public JobFacetsBuilder documentation(DocumentationJobFacet documentation) {
      this.documentation = documentation;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public JobFacetsBuilder put(String key, JobFacet value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of JobFacets from the fields set in the builder
     */
    @Override
    public JobFacets build() {
      JobFacets __result = new JobFacets(jobType, sourceCode, gcp_lineage, sql, lineage, gcp_composer_job, ownership, sourceCodeLocation, tags, documentation);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for CatalogDatasetFacet
   */
  @JsonDeserialize(
      as = CatalogDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "framework",
      "type",
      "name",
      "metadataUri",
      "warehouseUri",
      "source",
      "catalogProperties"
  })
  public static final class CatalogDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String framework;

    private final String type;

    private final String name;

    private final String metadataUri;

    private final String warehouseUri;

    private final String source;

    private final CatalogDatasetFacetCatalogProperties catalogProperties;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param framework The storage framework for which the catalog is configured
     * @param type Type of the catalog.
     * @param name Name of the catalog, as configured in the source system.
     * @param metadataUri URI or connection string to the catalog, if applicable.
     * @param warehouseUri URI or connection string to the physical location of the data that the catalog describes.
     * @param source Source system where the catalog is configured.
     * @param catalogProperties Additional catalog properties
     */
    @JsonCreator
    private CatalogDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("framework") String framework, @JsonProperty("type") String type,
        @JsonProperty("name") String name, @JsonProperty("metadataUri") String metadataUri,
        @JsonProperty("warehouseUri") String warehouseUri, @JsonProperty("source") String source,
        @JsonProperty("catalogProperties") CatalogDatasetFacetCatalogProperties catalogProperties) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-0/CatalogDatasetFacet.json#/$defs/CatalogDatasetFacet");
      this._deleted = null;
      this.framework = framework;
      this.type = type;
      this.name = name;
      this.metadataUri = metadataUri;
      this.warehouseUri = warehouseUri;
      this.source = source;
      this.catalogProperties = catalogProperties;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The storage framework for which the catalog is configured
     */
    public String getFramework() {
      return framework;
    }

    /**
     * @return Type of the catalog.
     */
    public String getType() {
      return type;
    }

    /**
     * @return Name of the catalog, as configured in the source system.
     */
    public String getName() {
      return name;
    }

    /**
     * @return URI or connection string to the catalog, if applicable.
     */
    public String getMetadataUri() {
      return metadataUri;
    }

    /**
     * @return URI or connection string to the physical location of the data that the catalog describes.
     */
    public String getWarehouseUri() {
      return warehouseUri;
    }

    /**
     * @return Source system where the catalog is configured.
     */
    public String getSource() {
      return source;
    }

    /**
     * @return Additional catalog properties
     */
    public CatalogDatasetFacetCatalogProperties getCatalogProperties() {
      return catalogProperties;
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
      CatalogDatasetFacet that = (CatalogDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(framework, that.framework)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(metadataUri, that.metadataUri)) return false;
      if (!Objects.equals(warehouseUri, that.warehouseUri)) return false;
      if (!Objects.equals(source, that.source)) return false;
      if (!Objects.equals(catalogProperties, that.catalogProperties)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, framework, type, name, metadataUri, warehouseUri, source, catalogProperties, additionalProperties);
    }
  }

  /**
   * builder class for CatalogDatasetFacet
   */
  public final class CatalogDatasetFacetBuilder implements Builder<CatalogDatasetFacet> {
    private String framework;

    private String type;

    private String name;

    private String metadataUri;

    private String warehouseUri;

    private String source;

    private CatalogDatasetFacetCatalogProperties catalogProperties;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param framework The storage framework for which the catalog is configured
     * @return this
     */
    public CatalogDatasetFacetBuilder framework(String framework) {
      this.framework = framework;
      return this;
    }

    /**
     * @param type Type of the catalog.
     * @return this
     */
    public CatalogDatasetFacetBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param name Name of the catalog, as configured in the source system.
     * @return this
     */
    public CatalogDatasetFacetBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param metadataUri URI or connection string to the catalog, if applicable.
     * @return this
     */
    public CatalogDatasetFacetBuilder metadataUri(String metadataUri) {
      this.metadataUri = metadataUri;
      return this;
    }

    /**
     * @param warehouseUri URI or connection string to the physical location of the data that the catalog describes.
     * @return this
     */
    public CatalogDatasetFacetBuilder warehouseUri(String warehouseUri) {
      this.warehouseUri = warehouseUri;
      return this;
    }

    /**
     * @param source Source system where the catalog is configured.
     * @return this
     */
    public CatalogDatasetFacetBuilder source(String source) {
      this.source = source;
      return this;
    }

    /**
     * @param catalogProperties Additional catalog properties
     * @return this
     */
    public CatalogDatasetFacetBuilder catalogProperties(
        CatalogDatasetFacetCatalogProperties catalogProperties) {
      this.catalogProperties = catalogProperties;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public CatalogDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of CatalogDatasetFacet from the fields set in the builder
     */
    @Override
    public CatalogDatasetFacet build() {
      CatalogDatasetFacet __result = new CatalogDatasetFacet(OpenLineage.this.producer, framework, type, name, metadataUri, warehouseUri, source, catalogProperties);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  public static class DefaultDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _deleted set to true to delete a facet
     */
    @JsonCreator
    public DefaultDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("_deleted") Boolean _deleted) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetFacet");
      this._deleted = _deleted;
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
    @Override
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
  }

  /**
   * Interface for DatasetFacet
   */
  @JsonDeserialize(
      as = DefaultDatasetFacet.class
  )
  public interface DatasetFacet {
    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    URI get_producer();

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    URI get_schemaURL();

    /**
     * @return set to true to delete a facet
     */
    Boolean get_deleted();

    /**
     * @return set to true to delete a facet
     */
    @JsonIgnore
    default Boolean isDeleted() {
      return get_deleted();
    }

    /**
     * @return additional properties
     */
    Map<String, Object> getAdditionalProperties();
  }

  /**
   * model class for OwnershipDatasetFacet
   */
  @JsonDeserialize(
      as = OwnershipDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "owners"
  })
  public static final class OwnershipDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<OwnershipDatasetFacetOwners> owners;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param owners The owners of the dataset.
     */
    @JsonCreator
    private OwnershipDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("owners") List<OwnershipDatasetFacetOwners> owners) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json#/$defs/OwnershipDatasetFacet");
      this._deleted = null;
      this.owners = owners;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The owners of the dataset.
     */
    public List<OwnershipDatasetFacetOwners> getOwners() {
      return owners;
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
      OwnershipDatasetFacet that = (OwnershipDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(owners, that.owners)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, owners, additionalProperties);
    }
  }

  /**
   * builder class for OwnershipDatasetFacet
   */
  public final class OwnershipDatasetFacetBuilder implements Builder<OwnershipDatasetFacet> {
    private List<OwnershipDatasetFacetOwners> owners;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param owners The owners of the dataset.
     * @return this
     */
    public OwnershipDatasetFacetBuilder owners(List<OwnershipDatasetFacetOwners> owners) {
      this.owners = owners;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public OwnershipDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of OwnershipDatasetFacet from the fields set in the builder
     */
    @Override
    public OwnershipDatasetFacet build() {
      OwnershipDatasetFacet __result = new OwnershipDatasetFacet(OpenLineage.this.producer, owners);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for EnvironmentVariablesRunFacet
   */
  @JsonDeserialize(
      as = EnvironmentVariablesRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "environmentVariables"
  })
  public static final class EnvironmentVariablesRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final List<EnvironmentVariable> environmentVariables;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param environmentVariables The environment variables for the run.
     */
    @JsonCreator
    private EnvironmentVariablesRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("environmentVariables") List<EnvironmentVariable> environmentVariables) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/EnvironmentVariablesRunFacet.json#/$defs/EnvironmentVariablesRunFacet");
      this.environmentVariables = environmentVariables;
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
     * @return The environment variables for the run.
     */
    public List<EnvironmentVariable> getEnvironmentVariables() {
      return environmentVariables;
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
      EnvironmentVariablesRunFacet that = (EnvironmentVariablesRunFacet) o;
      if (!Objects.equals(environmentVariables, that.environmentVariables)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(environmentVariables, additionalProperties);
    }
  }

  /**
   * builder class for EnvironmentVariablesRunFacet
   */
  public final class EnvironmentVariablesRunFacetBuilder implements Builder<EnvironmentVariablesRunFacet> {
    private List<EnvironmentVariable> environmentVariables;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param environmentVariables The environment variables for the run.
     * @return this
     */
    public EnvironmentVariablesRunFacetBuilder environmentVariables(
        List<EnvironmentVariable> environmentVariables) {
      this.environmentVariables = environmentVariables;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public EnvironmentVariablesRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of EnvironmentVariablesRunFacet from the fields set in the builder
     */
    @Override
    public EnvironmentVariablesRunFacet build() {
      EnvironmentVariablesRunFacet __result = new EnvironmentVariablesRunFacet(OpenLineage.this.producer, environmentVariables);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for JobDependenciesRunFacet
   */
  @JsonDeserialize(
      as = JobDependenciesRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "upstream",
      "downstream",
      "trigger_rule"
  })
  public static final class JobDependenciesRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final List<JobDependency> upstream;

    private final List<JobDependency> downstream;

    private final String trigger_rule;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param upstream Job runs that must complete before the current run can start.
     * @param downstream Job runs that will start after completion of the current run.
     * @param trigger_rule Specifies the condition under which this job will run based on the status of upstream jobs.
     */
    @JsonCreator
    private JobDependenciesRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("upstream") List<JobDependency> upstream,
        @JsonProperty("downstream") List<JobDependency> downstream,
        @JsonProperty("trigger_rule") String trigger_rule) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/JobDependenciesRunFacet.json#/$defs/JobDependenciesRunFacet");
      this.upstream = upstream;
      this.downstream = downstream;
      this.trigger_rule = trigger_rule;
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
     * @return Job runs that must complete before the current run can start.
     */
    public List<JobDependency> getUpstream() {
      return upstream;
    }

    /**
     * @return Job runs that will start after completion of the current run.
     */
    public List<JobDependency> getDownstream() {
      return downstream;
    }

    /**
     * @return Specifies the condition under which this job will run based on the status of upstream jobs.
     */
    public String getTrigger_rule() {
      return trigger_rule;
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
      JobDependenciesRunFacet that = (JobDependenciesRunFacet) o;
      if (!Objects.equals(upstream, that.upstream)) return false;
      if (!Objects.equals(downstream, that.downstream)) return false;
      if (!Objects.equals(trigger_rule, that.trigger_rule)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(upstream, downstream, trigger_rule, additionalProperties);
    }
  }

  /**
   * builder class for JobDependenciesRunFacet
   */
  public final class JobDependenciesRunFacetBuilder implements Builder<JobDependenciesRunFacet> {
    private List<JobDependency> upstream;

    private List<JobDependency> downstream;

    private String trigger_rule;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param upstream Job runs that must complete before the current run can start.
     * @return this
     */
    public JobDependenciesRunFacetBuilder upstream(List<JobDependency> upstream) {
      this.upstream = upstream;
      return this;
    }

    /**
     * @param downstream Job runs that will start after completion of the current run.
     * @return this
     */
    public JobDependenciesRunFacetBuilder downstream(List<JobDependency> downstream) {
      this.downstream = downstream;
      return this;
    }

    /**
     * @param trigger_rule Specifies the condition under which this job will run based on the status of upstream jobs.
     * @return this
     */
    public JobDependenciesRunFacetBuilder trigger_rule(String trigger_rule) {
      this.trigger_rule = trigger_rule;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public JobDependenciesRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of JobDependenciesRunFacet from the fields set in the builder
     */
    @Override
    public JobDependenciesRunFacet build() {
      JobDependenciesRunFacet __result = new JobDependenciesRunFacet(OpenLineage.this.producer, upstream, downstream, trigger_rule);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DatasetFacets
   */
  @JsonDeserialize(
      as = DatasetFacets.class
  )
  @JsonPropertyOrder({
      "hierarchy",
      "dataSource",
      "version",
      "datasetType",
      "storage",
      "columnLineage",
      "lifecycleStateChange",
      "dataQualityMetrics",
      "lineage",
      "tags",
      "documentation",
      "schema",
      "ownership",
      "catalog",
      "symlinks"
  })
  public static final class DatasetFacets {
    private final HierarchyDatasetFacet hierarchy;

    private final DatasourceDatasetFacet dataSource;

    private final DatasetVersionDatasetFacet version;

    private final DatasetTypeDatasetFacet datasetType;

    private final StorageDatasetFacet storage;

    private final ColumnLineageDatasetFacet columnLineage;

    private final LifecycleStateChangeDatasetFacet lifecycleStateChange;

    private final DataQualityMetricsDatasetFacet dataQualityMetrics;

    private final LineageDatasetFacet lineage;

    private final TagsDatasetFacet tags;

    private final DocumentationDatasetFacet documentation;

    private final SchemaDatasetFacet schema;

    private final OwnershipDatasetFacet ownership;

    private final CatalogDatasetFacet catalog;

    private final SymlinksDatasetFacet symlinks;

    @JsonAnySetter
    private final Map<String, DatasetFacet> additionalProperties;

    /**
     * @param hierarchy the hierarchy
     * @param dataSource the dataSource
     * @param version the version
     * @param datasetType the datasetType
     * @param storage the storage
     * @param columnLineage the columnLineage
     * @param lifecycleStateChange the lifecycleStateChange
     * @param dataQualityMetrics the dataQualityMetrics
     * @param lineage the lineage
     * @param tags the tags
     * @param documentation the documentation
     * @param schema the schema
     * @param ownership the ownership
     * @param catalog the catalog
     * @param symlinks the symlinks
     */
    @JsonCreator
    private DatasetFacets(@JsonProperty("hierarchy") HierarchyDatasetFacet hierarchy,
        @JsonProperty("dataSource") DatasourceDatasetFacet dataSource,
        @JsonProperty("version") DatasetVersionDatasetFacet version,
        @JsonProperty("datasetType") DatasetTypeDatasetFacet datasetType,
        @JsonProperty("storage") StorageDatasetFacet storage,
        @JsonProperty("columnLineage") ColumnLineageDatasetFacet columnLineage,
        @JsonProperty("lifecycleStateChange") LifecycleStateChangeDatasetFacet lifecycleStateChange,
        @JsonProperty("dataQualityMetrics") DataQualityMetricsDatasetFacet dataQualityMetrics,
        @JsonProperty("lineage") LineageDatasetFacet lineage,
        @JsonProperty("tags") TagsDatasetFacet tags,
        @JsonProperty("documentation") DocumentationDatasetFacet documentation,
        @JsonProperty("schema") SchemaDatasetFacet schema,
        @JsonProperty("ownership") OwnershipDatasetFacet ownership,
        @JsonProperty("catalog") CatalogDatasetFacet catalog,
        @JsonProperty("symlinks") SymlinksDatasetFacet symlinks) {
      this.hierarchy = hierarchy;
      this.dataSource = dataSource;
      this.version = version;
      this.datasetType = datasetType;
      this.storage = storage;
      this.columnLineage = columnLineage;
      this.lifecycleStateChange = lifecycleStateChange;
      this.dataQualityMetrics = dataQualityMetrics;
      this.lineage = lineage;
      this.tags = tags;
      this.documentation = documentation;
      this.schema = schema;
      this.ownership = ownership;
      this.catalog = catalog;
      this.symlinks = symlinks;
      this.additionalProperties = new LinkedHashMap<>();
    }

    public HierarchyDatasetFacet getHierarchy() {
      return hierarchy;
    }

    public DatasourceDatasetFacet getDataSource() {
      return dataSource;
    }

    public DatasetVersionDatasetFacet getVersion() {
      return version;
    }

    public DatasetTypeDatasetFacet getDatasetType() {
      return datasetType;
    }

    public StorageDatasetFacet getStorage() {
      return storage;
    }

    public ColumnLineageDatasetFacet getColumnLineage() {
      return columnLineage;
    }

    public LifecycleStateChangeDatasetFacet getLifecycleStateChange() {
      return lifecycleStateChange;
    }

    public DataQualityMetricsDatasetFacet getDataQualityMetrics() {
      return dataQualityMetrics;
    }

    public LineageDatasetFacet getLineage() {
      return lineage;
    }

    public TagsDatasetFacet getTags() {
      return tags;
    }

    public DocumentationDatasetFacet getDocumentation() {
      return documentation;
    }

    public SchemaDatasetFacet getSchema() {
      return schema;
    }

    public OwnershipDatasetFacet getOwnership() {
      return ownership;
    }

    public CatalogDatasetFacet getCatalog() {
      return catalog;
    }

    public SymlinksDatasetFacet getSymlinks() {
      return symlinks;
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
      if (!Objects.equals(hierarchy, that.hierarchy)) return false;
      if (!Objects.equals(dataSource, that.dataSource)) return false;
      if (!Objects.equals(version, that.version)) return false;
      if (!Objects.equals(datasetType, that.datasetType)) return false;
      if (!Objects.equals(storage, that.storage)) return false;
      if (!Objects.equals(columnLineage, that.columnLineage)) return false;
      if (!Objects.equals(lifecycleStateChange, that.lifecycleStateChange)) return false;
      if (!Objects.equals(dataQualityMetrics, that.dataQualityMetrics)) return false;
      if (!Objects.equals(lineage, that.lineage)) return false;
      if (!Objects.equals(tags, that.tags)) return false;
      if (!Objects.equals(documentation, that.documentation)) return false;
      if (!Objects.equals(schema, that.schema)) return false;
      if (!Objects.equals(ownership, that.ownership)) return false;
      if (!Objects.equals(catalog, that.catalog)) return false;
      if (!Objects.equals(symlinks, that.symlinks)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(hierarchy, dataSource, version, datasetType, storage, columnLineage, lifecycleStateChange, dataQualityMetrics, lineage, tags, documentation, schema, ownership, catalog, symlinks, additionalProperties);
    }
  }

  /**
   * builder class for DatasetFacets
   */
  public static final class DatasetFacetsBuilder implements Builder<DatasetFacets> {
    private HierarchyDatasetFacet hierarchy;

    private DatasourceDatasetFacet dataSource;

    private DatasetVersionDatasetFacet version;

    private DatasetTypeDatasetFacet datasetType;

    private StorageDatasetFacet storage;

    private ColumnLineageDatasetFacet columnLineage;

    private LifecycleStateChangeDatasetFacet lifecycleStateChange;

    private DataQualityMetricsDatasetFacet dataQualityMetrics;

    private LineageDatasetFacet lineage;

    private TagsDatasetFacet tags;

    private DocumentationDatasetFacet documentation;

    private SchemaDatasetFacet schema;

    private OwnershipDatasetFacet ownership;

    private CatalogDatasetFacet catalog;

    private SymlinksDatasetFacet symlinks;

    private final Map<String, DatasetFacet> additionalProperties = new LinkedHashMap<>();

    /**
     * @param hierarchy the hierarchy
     * @return this
     */
    public DatasetFacetsBuilder hierarchy(HierarchyDatasetFacet hierarchy) {
      this.hierarchy = hierarchy;
      return this;
    }

    /**
     * @param dataSource the dataSource
     * @return this
     */
    public DatasetFacetsBuilder dataSource(DatasourceDatasetFacet dataSource) {
      this.dataSource = dataSource;
      return this;
    }

    /**
     * @param version the version
     * @return this
     */
    public DatasetFacetsBuilder version(DatasetVersionDatasetFacet version) {
      this.version = version;
      return this;
    }

    /**
     * @param datasetType the datasetType
     * @return this
     */
    public DatasetFacetsBuilder datasetType(DatasetTypeDatasetFacet datasetType) {
      this.datasetType = datasetType;
      return this;
    }

    /**
     * @param storage the storage
     * @return this
     */
    public DatasetFacetsBuilder storage(StorageDatasetFacet storage) {
      this.storage = storage;
      return this;
    }

    /**
     * @param columnLineage the columnLineage
     * @return this
     */
    public DatasetFacetsBuilder columnLineage(ColumnLineageDatasetFacet columnLineage) {
      this.columnLineage = columnLineage;
      return this;
    }

    /**
     * @param lifecycleStateChange the lifecycleStateChange
     * @return this
     */
    public DatasetFacetsBuilder lifecycleStateChange(
        LifecycleStateChangeDatasetFacet lifecycleStateChange) {
      this.lifecycleStateChange = lifecycleStateChange;
      return this;
    }

    /**
     * @param dataQualityMetrics the dataQualityMetrics
     * @return this
     */
    public DatasetFacetsBuilder dataQualityMetrics(
        DataQualityMetricsDatasetFacet dataQualityMetrics) {
      this.dataQualityMetrics = dataQualityMetrics;
      return this;
    }

    /**
     * @param lineage the lineage
     * @return this
     */
    public DatasetFacetsBuilder lineage(LineageDatasetFacet lineage) {
      this.lineage = lineage;
      return this;
    }

    /**
     * @param tags the tags
     * @return this
     */
    public DatasetFacetsBuilder tags(TagsDatasetFacet tags) {
      this.tags = tags;
      return this;
    }

    /**
     * @param documentation the documentation
     * @return this
     */
    public DatasetFacetsBuilder documentation(DocumentationDatasetFacet documentation) {
      this.documentation = documentation;
      return this;
    }

    /**
     * @param schema the schema
     * @return this
     */
    public DatasetFacetsBuilder schema(SchemaDatasetFacet schema) {
      this.schema = schema;
      return this;
    }

    /**
     * @param ownership the ownership
     * @return this
     */
    public DatasetFacetsBuilder ownership(OwnershipDatasetFacet ownership) {
      this.ownership = ownership;
      return this;
    }

    /**
     * @param catalog the catalog
     * @return this
     */
    public DatasetFacetsBuilder catalog(CatalogDatasetFacet catalog) {
      this.catalog = catalog;
      return this;
    }

    /**
     * @param symlinks the symlinks
     * @return this
     */
    public DatasetFacetsBuilder symlinks(SymlinksDatasetFacet symlinks) {
      this.symlinks = symlinks;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DatasetFacetsBuilder put(String key, DatasetFacet value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DatasetFacets from the fields set in the builder
     */
    @Override
    public DatasetFacets build() {
      DatasetFacets __result = new DatasetFacets(hierarchy, dataSource, version, datasetType, storage, columnLineage, lifecycleStateChange, dataQualityMetrics, lineage, tags, documentation, schema, ownership, catalog, symlinks);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for GcpComposerRunFacet
   */
  @JsonDeserialize(
      as = GcpComposerRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "dagRunId"
  })
  public static final class GcpComposerRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final String dagRunId;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param dagRunId The id of the DAG run
     */
    @JsonCreator
    private GcpComposerRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("dagRunId") String dagRunId) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/GcpComposerRunFacet.json#/$defs/GcpComposerRunFacet");
      this.dagRunId = dagRunId;
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
     * @return The id of the DAG run
     */
    public String getDagRunId() {
      return dagRunId;
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
      GcpComposerRunFacet that = (GcpComposerRunFacet) o;
      if (!Objects.equals(dagRunId, that.dagRunId)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(dagRunId, additionalProperties);
    }
  }

  /**
   * builder class for GcpComposerRunFacet
   */
  public final class GcpComposerRunFacetBuilder implements Builder<GcpComposerRunFacet> {
    private String dagRunId;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param dagRunId The id of the DAG run
     * @return this
     */
    public GcpComposerRunFacetBuilder dagRunId(String dagRunId) {
      this.dagRunId = dagRunId;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public GcpComposerRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of GcpComposerRunFacet from the fields set in the builder
     */
    @Override
    public GcpComposerRunFacet build() {
      GcpComposerRunFacet __result = new GcpComposerRunFacet(OpenLineage.this.producer, dagRunId);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for TestExecutionParams
   */
  @JsonDeserialize(
      as = TestExecutionParams.class
  )
  @JsonPropertyOrder
  public static final class TestExecutionParams {
    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    @JsonCreator
    private TestExecutionParams() {
      this.additionalProperties = new LinkedHashMap<>();
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
      TestExecutionParams that = (TestExecutionParams) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for TestExecutionParams
   */
  public static final class TestExecutionParamsBuilder implements Builder<TestExecutionParams> {
    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public TestExecutionParamsBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of TestExecutionParams from the fields set in the builder
     */
    @Override
    public TestExecutionParams build() {
      TestExecutionParams __result = new TestExecutionParams();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for OwnershipJobFacet
   */
  @JsonDeserialize(
      as = OwnershipJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "owners"
  })
  public static final class OwnershipJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<OwnershipJobFacetOwners> owners;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param owners The owners of the job.
     */
    @JsonCreator
    private OwnershipJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("owners") List<OwnershipJobFacetOwners> owners) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json#/$defs/OwnershipJobFacet");
      this._deleted = null;
      this.owners = owners;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The owners of the job.
     */
    public List<OwnershipJobFacetOwners> getOwners() {
      return owners;
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
      OwnershipJobFacet that = (OwnershipJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(owners, that.owners)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, owners, additionalProperties);
    }
  }

  /**
   * builder class for OwnershipJobFacet
   */
  public final class OwnershipJobFacetBuilder implements Builder<OwnershipJobFacet> {
    private List<OwnershipJobFacetOwners> owners;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param owners The owners of the job.
     * @return this
     */
    public OwnershipJobFacetBuilder owners(List<OwnershipJobFacetOwners> owners) {
      this.owners = owners;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public OwnershipJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of OwnershipJobFacet from the fields set in the builder
     */
    @Override
    public OwnershipJobFacet build() {
      OwnershipJobFacet __result = new OwnershipJobFacet(OpenLineage.this.producer, owners);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for RunIdentifier
   */
  @JsonDeserialize(
      as = RunIdentifier.class
  )
  @JsonPropertyOrder("runId")
  public static final class RunIdentifier {
    private final UUID runId;

    /**
     * @param runId The globally unique ID of the run.
     */
    @JsonCreator
    private RunIdentifier(@JsonProperty("runId") UUID runId) {
      this.runId = runId;
    }

    /**
     * @return The globally unique ID of the run.
     */
    public UUID getRunId() {
      return runId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RunIdentifier that = (RunIdentifier) o;
      if (!Objects.equals(runId, that.runId)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(runId);
    }
  }

  /**
   * builder class for RunIdentifier
   */
  public static final class RunIdentifierBuilder implements Builder<RunIdentifier> {
    private UUID runId;

    /**
     * @param runId The globally unique ID of the run.
     * @return this
     */
    public RunIdentifierBuilder runId(UUID runId) {
      this.runId = runId;
      return this;
    }

    /**
     * build an instance of RunIdentifier from the fields set in the builder
     */
    @Override
    public RunIdentifier build() {
      RunIdentifier __result = new RunIdentifier(runId);
      return __result;
    }
  }

  /**
   * model class for FieldBaseCompareExpression
   */
  @JsonDeserialize(
      as = FieldBaseCompareExpression.class
  )
  @JsonPropertyOrder({
      "type",
      "field"
  })
  public static final class FieldBaseCompareExpression {
    private final String type;

    private final String field;

    /**
     * @param field the field
     */
    @JsonCreator
    private FieldBaseCompareExpression(@JsonProperty("field") String field) {
      this.type = "field";
      this.field = field;
    }

    public String getType() {
      return type;
    }

    public String getField() {
      return field;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldBaseCompareExpression that = (FieldBaseCompareExpression) o;
      if (!Objects.equals(field, that.field)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(field);
    }
  }

  /**
   * builder class for FieldBaseCompareExpression
   */
  public static final class FieldBaseCompareExpressionBuilder implements Builder<FieldBaseCompareExpression> {
    private String field;

    /**
     * @param field the field
     * @return this
     */
    public FieldBaseCompareExpressionBuilder field(String field) {
      this.field = field;
      return this;
    }

    /**
     * build an instance of FieldBaseCompareExpression from the fields set in the builder
     */
    @Override
    public FieldBaseCompareExpression build() {
      FieldBaseCompareExpression __result = new FieldBaseCompareExpression(field);
      return __result;
    }
  }

  /**
   * model class for IcebergCommitReportOutputDatasetFacetMetadata
   */
  @JsonDeserialize(
      as = IcebergCommitReportOutputDatasetFacetMetadata.class
  )
  @JsonPropertyOrder
  public static final class IcebergCommitReportOutputDatasetFacetMetadata {
    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    @JsonCreator
    private IcebergCommitReportOutputDatasetFacetMetadata() {
      this.additionalProperties = new LinkedHashMap<>();
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
      IcebergCommitReportOutputDatasetFacetMetadata that = (IcebergCommitReportOutputDatasetFacetMetadata) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for IcebergCommitReportOutputDatasetFacetMetadata
   */
  public static final class IcebergCommitReportOutputDatasetFacetMetadataBuilder implements Builder<IcebergCommitReportOutputDatasetFacetMetadata> {
    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetMetadataBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of IcebergCommitReportOutputDatasetFacetMetadata from the fields set in the builder
     */
    @Override
    public IcebergCommitReportOutputDatasetFacetMetadata build() {
      IcebergCommitReportOutputDatasetFacetMetadata __result = new IcebergCommitReportOutputDatasetFacetMetadata();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ParentRunFacetJob
   */
  @JsonDeserialize(
      as = ParentRunFacetJob.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name"
  })
  public static final class ParentRunFacetJob {
    private final String namespace;

    private final String name;

    /**
     * @param namespace The namespace containing that job
     * @param name The unique name for that job within that namespace
     */
    @JsonCreator
    private ParentRunFacetJob(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name) {
      this.namespace = namespace;
      this.name = name;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ParentRunFacetJob that = (ParentRunFacetJob) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name);
    }
  }

  /**
   * builder class for ParentRunFacetJob
   */
  public static final class ParentRunFacetJobBuilder implements Builder<ParentRunFacetJob> {
    private String namespace;

    private String name;

    /**
     * @param namespace The namespace containing that job
     * @return this
     */
    public ParentRunFacetJobBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The unique name for that job within that namespace
     * @return this
     */
    public ParentRunFacetJobBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * build an instance of ParentRunFacetJob from the fields set in the builder
     */
    @Override
    public ParentRunFacetJob build() {
      ParentRunFacetJob __result = new ParentRunFacetJob(namespace, name);
      return __result;
    }
  }

  /**
   * model class for GcpLineageJobFacetOrigin
   */
  @JsonDeserialize(
      as = GcpLineageJobFacetOrigin.class
  )
  @JsonPropertyOrder({
      "sourceType",
      "name"
  })
  public static final class GcpLineageJobFacetOrigin {
    private final String sourceType;

    private final String name;

    /**
     * @param sourceType Type of the source. Possible values can be found in GCP documentation (https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#SourceType) 
     * @param name If the sourceType isn't CUSTOM, the value of this field should be a GCP resource name of the system, which reports lineage. The project and location parts of the resource name must match the project and location of the lineage resource being created. More details in GCP documentation https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#origin
     */
    @JsonCreator
    private GcpLineageJobFacetOrigin(@JsonProperty("sourceType") String sourceType,
        @JsonProperty("name") String name) {
      this.sourceType = sourceType;
      this.name = name;
    }

    /**
     * @return Type of the source. Possible values can be found in GCP documentation (https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#SourceType) 
     */
    public String getSourceType() {
      return sourceType;
    }

    /**
     * @return If the sourceType isn't CUSTOM, the value of this field should be a GCP resource name of the system, which reports lineage. The project and location parts of the resource name must match the project and location of the lineage resource being created. More details in GCP documentation https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#origin
     */
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GcpLineageJobFacetOrigin that = (GcpLineageJobFacetOrigin) o;
      if (!Objects.equals(sourceType, that.sourceType)) return false;
      if (!Objects.equals(name, that.name)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceType, name);
    }
  }

  /**
   * builder class for GcpLineageJobFacetOrigin
   */
  public static final class GcpLineageJobFacetOriginBuilder implements Builder<GcpLineageJobFacetOrigin> {
    private String sourceType;

    private String name;

    /**
     * @param sourceType Type of the source. Possible values can be found in GCP documentation (https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#SourceType) 
     * @return this
     */
    public GcpLineageJobFacetOriginBuilder sourceType(String sourceType) {
      this.sourceType = sourceType;
      return this;
    }

    /**
     * @param name If the sourceType isn't CUSTOM, the value of this field should be a GCP resource name of the system, which reports lineage. The project and location parts of the resource name must match the project and location of the lineage resource being created. More details in GCP documentation https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest/v1/projects.locations.processes#origin
     * @return this
     */
    public GcpLineageJobFacetOriginBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * build an instance of GcpLineageJobFacetOrigin from the fields set in the builder
     */
    @Override
    public GcpLineageJobFacetOrigin build() {
      GcpLineageJobFacetOrigin __result = new GcpLineageJobFacetOrigin(sourceType, name);
      return __result;
    }
  }

  /**
   * model class for ErrorMessageRunFacet
   */
  @JsonDeserialize(
      as = ErrorMessageRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "message",
      "programmingLanguage",
      "stackTrace"
  })
  public static final class ErrorMessageRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final String message;

    private final String programmingLanguage;

    private final String stackTrace;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param message A human-readable string representing error message generated by observed system
     * @param programmingLanguage Programming language the observed system uses.
     * @param stackTrace A language-specific stack trace generated by observed system
     */
    @JsonCreator
    private ErrorMessageRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("message") String message,
        @JsonProperty("programmingLanguage") String programmingLanguage,
        @JsonProperty("stackTrace") String stackTrace) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet");
      this.message = message;
      this.programmingLanguage = programmingLanguage;
      this.stackTrace = stackTrace;
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
     * @return A human-readable string representing error message generated by observed system
     */
    public String getMessage() {
      return message;
    }

    /**
     * @return Programming language the observed system uses.
     */
    public String getProgrammingLanguage() {
      return programmingLanguage;
    }

    /**
     * @return A language-specific stack trace generated by observed system
     */
    public String getStackTrace() {
      return stackTrace;
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
      ErrorMessageRunFacet that = (ErrorMessageRunFacet) o;
      if (!Objects.equals(message, that.message)) return false;
      if (!Objects.equals(programmingLanguage, that.programmingLanguage)) return false;
      if (!Objects.equals(stackTrace, that.stackTrace)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(message, programmingLanguage, stackTrace, additionalProperties);
    }
  }

  /**
   * builder class for ErrorMessageRunFacet
   */
  public final class ErrorMessageRunFacetBuilder implements Builder<ErrorMessageRunFacet> {
    private String message;

    private String programmingLanguage;

    private String stackTrace;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param message A human-readable string representing error message generated by observed system
     * @return this
     */
    public ErrorMessageRunFacetBuilder message(String message) {
      this.message = message;
      return this;
    }

    /**
     * @param programmingLanguage Programming language the observed system uses.
     * @return this
     */
    public ErrorMessageRunFacetBuilder programmingLanguage(String programmingLanguage) {
      this.programmingLanguage = programmingLanguage;
      return this;
    }

    /**
     * @param stackTrace A language-specific stack trace generated by observed system
     * @return this
     */
    public ErrorMessageRunFacetBuilder stackTrace(String stackTrace) {
      this.stackTrace = stackTrace;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ErrorMessageRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ErrorMessageRunFacet from the fields set in the builder
     */
    @Override
    public ErrorMessageRunFacet build() {
      ErrorMessageRunFacet __result = new ErrorMessageRunFacet(OpenLineage.this.producer, message, programmingLanguage, stackTrace);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for IcebergCommitReportOutputDatasetFacetCommitMetrics
   */
  @JsonDeserialize(
      as = IcebergCommitReportOutputDatasetFacetCommitMetrics.class
  )
  @JsonPropertyOrder({
      "totalDuration",
      "attempts",
      "addedDataFiles",
      "removedDataFiles",
      "totalDataFiles",
      "addedDeleteFiles",
      "addedEqualityDeleteFiles",
      "addedPositionalDeleteFiles",
      "addedDVs",
      "removedDeleteFiles",
      "removedEqualityDeleteFiles",
      "removedPositionalDeleteFiles",
      "removedDVs",
      "totalDeleteFiles",
      "addedRecords",
      "removedRecords",
      "totalRecords",
      "addedFilesSizeInBytes",
      "removedFilesSizeInBytes",
      "totalFilesSizeInBytes",
      "addedPositionalDeletes",
      "removedPositionalDeletes",
      "totalPositionalDeletes",
      "addedEqualityDeletes",
      "removedEqualityDeletes",
      "totalEqualityDeletes"
  })
  public static final class IcebergCommitReportOutputDatasetFacetCommitMetrics {
    private final Double totalDuration;

    private final Double attempts;

    private final Double addedDataFiles;

    private final Double removedDataFiles;

    private final Double totalDataFiles;

    private final Double addedDeleteFiles;

    private final Double addedEqualityDeleteFiles;

    private final Double addedPositionalDeleteFiles;

    private final Double addedDVs;

    private final Double removedDeleteFiles;

    private final Double removedEqualityDeleteFiles;

    private final Double removedPositionalDeleteFiles;

    private final Double removedDVs;

    private final Double totalDeleteFiles;

    private final Double addedRecords;

    private final Double removedRecords;

    private final Double totalRecords;

    private final Double addedFilesSizeInBytes;

    private final Double removedFilesSizeInBytes;

    private final Double totalFilesSizeInBytes;

    private final Double addedPositionalDeletes;

    private final Double removedPositionalDeletes;

    private final Double totalPositionalDeletes;

    private final Double addedEqualityDeletes;

    private final Double removedEqualityDeletes;

    private final Double totalEqualityDeletes;

    /**
     * @param totalDuration Duration of the commit in MILLISECONDS
     * @param attempts Number of attempts made to commit the iceberg table
     * @param addedDataFiles Number of data files that are added during the commit
     * @param removedDataFiles Number of data files that are removed during the commit
     * @param totalDataFiles Total number of data files that are present in the iceberg table
     * @param addedDeleteFiles Number of delete files that are added during the commit
     * @param addedEqualityDeleteFiles Number of added equality delete files
     * @param addedPositionalDeleteFiles Number of added positional delete files
     * @param addedDVs Number of added DVs
     * @param removedDeleteFiles Number of delete files that are removed during the commit
     * @param removedEqualityDeleteFiles Number of removed equality delete files
     * @param removedPositionalDeleteFiles Number of removed positional delete files
     * @param removedDVs Number of removed DVs
     * @param totalDeleteFiles Total number of temporary delete files that are present in the iceberg table
     * @param addedRecords Number of records that are added during the commit
     * @param removedRecords Number of records that are removed during the commit
     * @param totalRecords Number of records that are present in the iceberg table
     * @param addedFilesSizeInBytes Number of files size in bytes that are added during the commit
     * @param removedFilesSizeInBytes Number of files size in bytes that are removed during the commit
     * @param totalFilesSizeInBytes Number of files size in bytes in the iceberg table
     * @param addedPositionalDeletes Number of positional deletes that are added during the commit
     * @param removedPositionalDeletes Number of positional deletes that are removed during the commit
     * @param totalPositionalDeletes Number of positional deletes that are present in the iceberg table
     * @param addedEqualityDeletes Number of equality deletes that are added during the commit
     * @param removedEqualityDeletes Number of equality deletes that are removed during the commit
     * @param totalEqualityDeletes Number of equality deletes that are present in the iceberg table
     */
    @JsonCreator
    private IcebergCommitReportOutputDatasetFacetCommitMetrics(
        @JsonProperty("totalDuration") Double totalDuration,
        @JsonProperty("attempts") Double attempts,
        @JsonProperty("addedDataFiles") Double addedDataFiles,
        @JsonProperty("removedDataFiles") Double removedDataFiles,
        @JsonProperty("totalDataFiles") Double totalDataFiles,
        @JsonProperty("addedDeleteFiles") Double addedDeleteFiles,
        @JsonProperty("addedEqualityDeleteFiles") Double addedEqualityDeleteFiles,
        @JsonProperty("addedPositionalDeleteFiles") Double addedPositionalDeleteFiles,
        @JsonProperty("addedDVs") Double addedDVs,
        @JsonProperty("removedDeleteFiles") Double removedDeleteFiles,
        @JsonProperty("removedEqualityDeleteFiles") Double removedEqualityDeleteFiles,
        @JsonProperty("removedPositionalDeleteFiles") Double removedPositionalDeleteFiles,
        @JsonProperty("removedDVs") Double removedDVs,
        @JsonProperty("totalDeleteFiles") Double totalDeleteFiles,
        @JsonProperty("addedRecords") Double addedRecords,
        @JsonProperty("removedRecords") Double removedRecords,
        @JsonProperty("totalRecords") Double totalRecords,
        @JsonProperty("addedFilesSizeInBytes") Double addedFilesSizeInBytes,
        @JsonProperty("removedFilesSizeInBytes") Double removedFilesSizeInBytes,
        @JsonProperty("totalFilesSizeInBytes") Double totalFilesSizeInBytes,
        @JsonProperty("addedPositionalDeletes") Double addedPositionalDeletes,
        @JsonProperty("removedPositionalDeletes") Double removedPositionalDeletes,
        @JsonProperty("totalPositionalDeletes") Double totalPositionalDeletes,
        @JsonProperty("addedEqualityDeletes") Double addedEqualityDeletes,
        @JsonProperty("removedEqualityDeletes") Double removedEqualityDeletes,
        @JsonProperty("totalEqualityDeletes") Double totalEqualityDeletes) {
      this.totalDuration = totalDuration;
      this.attempts = attempts;
      this.addedDataFiles = addedDataFiles;
      this.removedDataFiles = removedDataFiles;
      this.totalDataFiles = totalDataFiles;
      this.addedDeleteFiles = addedDeleteFiles;
      this.addedEqualityDeleteFiles = addedEqualityDeleteFiles;
      this.addedPositionalDeleteFiles = addedPositionalDeleteFiles;
      this.addedDVs = addedDVs;
      this.removedDeleteFiles = removedDeleteFiles;
      this.removedEqualityDeleteFiles = removedEqualityDeleteFiles;
      this.removedPositionalDeleteFiles = removedPositionalDeleteFiles;
      this.removedDVs = removedDVs;
      this.totalDeleteFiles = totalDeleteFiles;
      this.addedRecords = addedRecords;
      this.removedRecords = removedRecords;
      this.totalRecords = totalRecords;
      this.addedFilesSizeInBytes = addedFilesSizeInBytes;
      this.removedFilesSizeInBytes = removedFilesSizeInBytes;
      this.totalFilesSizeInBytes = totalFilesSizeInBytes;
      this.addedPositionalDeletes = addedPositionalDeletes;
      this.removedPositionalDeletes = removedPositionalDeletes;
      this.totalPositionalDeletes = totalPositionalDeletes;
      this.addedEqualityDeletes = addedEqualityDeletes;
      this.removedEqualityDeletes = removedEqualityDeletes;
      this.totalEqualityDeletes = totalEqualityDeletes;
    }

    /**
     * @return Duration of the commit in MILLISECONDS
     */
    public Double getTotalDuration() {
      return totalDuration;
    }

    /**
     * @return Number of attempts made to commit the iceberg table
     */
    public Double getAttempts() {
      return attempts;
    }

    /**
     * @return Number of data files that are added during the commit
     */
    public Double getAddedDataFiles() {
      return addedDataFiles;
    }

    /**
     * @return Number of data files that are removed during the commit
     */
    public Double getRemovedDataFiles() {
      return removedDataFiles;
    }

    /**
     * @return Total number of data files that are present in the iceberg table
     */
    public Double getTotalDataFiles() {
      return totalDataFiles;
    }

    /**
     * @return Number of delete files that are added during the commit
     */
    public Double getAddedDeleteFiles() {
      return addedDeleteFiles;
    }

    /**
     * @return Number of added equality delete files
     */
    public Double getAddedEqualityDeleteFiles() {
      return addedEqualityDeleteFiles;
    }

    /**
     * @return Number of added positional delete files
     */
    public Double getAddedPositionalDeleteFiles() {
      return addedPositionalDeleteFiles;
    }

    /**
     * @return Number of added DVs
     */
    public Double getAddedDVs() {
      return addedDVs;
    }

    /**
     * @return Number of delete files that are removed during the commit
     */
    public Double getRemovedDeleteFiles() {
      return removedDeleteFiles;
    }

    /**
     * @return Number of removed equality delete files
     */
    public Double getRemovedEqualityDeleteFiles() {
      return removedEqualityDeleteFiles;
    }

    /**
     * @return Number of removed positional delete files
     */
    public Double getRemovedPositionalDeleteFiles() {
      return removedPositionalDeleteFiles;
    }

    /**
     * @return Number of removed DVs
     */
    public Double getRemovedDVs() {
      return removedDVs;
    }

    /**
     * @return Total number of temporary delete files that are present in the iceberg table
     */
    public Double getTotalDeleteFiles() {
      return totalDeleteFiles;
    }

    /**
     * @return Number of records that are added during the commit
     */
    public Double getAddedRecords() {
      return addedRecords;
    }

    /**
     * @return Number of records that are removed during the commit
     */
    public Double getRemovedRecords() {
      return removedRecords;
    }

    /**
     * @return Number of records that are present in the iceberg table
     */
    public Double getTotalRecords() {
      return totalRecords;
    }

    /**
     * @return Number of files size in bytes that are added during the commit
     */
    public Double getAddedFilesSizeInBytes() {
      return addedFilesSizeInBytes;
    }

    /**
     * @return Number of files size in bytes that are removed during the commit
     */
    public Double getRemovedFilesSizeInBytes() {
      return removedFilesSizeInBytes;
    }

    /**
     * @return Number of files size in bytes in the iceberg table
     */
    public Double getTotalFilesSizeInBytes() {
      return totalFilesSizeInBytes;
    }

    /**
     * @return Number of positional deletes that are added during the commit
     */
    public Double getAddedPositionalDeletes() {
      return addedPositionalDeletes;
    }

    /**
     * @return Number of positional deletes that are removed during the commit
     */
    public Double getRemovedPositionalDeletes() {
      return removedPositionalDeletes;
    }

    /**
     * @return Number of positional deletes that are present in the iceberg table
     */
    public Double getTotalPositionalDeletes() {
      return totalPositionalDeletes;
    }

    /**
     * @return Number of equality deletes that are added during the commit
     */
    public Double getAddedEqualityDeletes() {
      return addedEqualityDeletes;
    }

    /**
     * @return Number of equality deletes that are removed during the commit
     */
    public Double getRemovedEqualityDeletes() {
      return removedEqualityDeletes;
    }

    /**
     * @return Number of equality deletes that are present in the iceberg table
     */
    public Double getTotalEqualityDeletes() {
      return totalEqualityDeletes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IcebergCommitReportOutputDatasetFacetCommitMetrics that = (IcebergCommitReportOutputDatasetFacetCommitMetrics) o;
      if (!Objects.equals(totalDuration, that.totalDuration)) return false;
      if (!Objects.equals(attempts, that.attempts)) return false;
      if (!Objects.equals(addedDataFiles, that.addedDataFiles)) return false;
      if (!Objects.equals(removedDataFiles, that.removedDataFiles)) return false;
      if (!Objects.equals(totalDataFiles, that.totalDataFiles)) return false;
      if (!Objects.equals(addedDeleteFiles, that.addedDeleteFiles)) return false;
      if (!Objects.equals(addedEqualityDeleteFiles, that.addedEqualityDeleteFiles)) return false;
      if (!Objects.equals(addedPositionalDeleteFiles, that.addedPositionalDeleteFiles)) return false;
      if (!Objects.equals(addedDVs, that.addedDVs)) return false;
      if (!Objects.equals(removedDeleteFiles, that.removedDeleteFiles)) return false;
      if (!Objects.equals(removedEqualityDeleteFiles, that.removedEqualityDeleteFiles)) return false;
      if (!Objects.equals(removedPositionalDeleteFiles, that.removedPositionalDeleteFiles)) return false;
      if (!Objects.equals(removedDVs, that.removedDVs)) return false;
      if (!Objects.equals(totalDeleteFiles, that.totalDeleteFiles)) return false;
      if (!Objects.equals(addedRecords, that.addedRecords)) return false;
      if (!Objects.equals(removedRecords, that.removedRecords)) return false;
      if (!Objects.equals(totalRecords, that.totalRecords)) return false;
      if (!Objects.equals(addedFilesSizeInBytes, that.addedFilesSizeInBytes)) return false;
      if (!Objects.equals(removedFilesSizeInBytes, that.removedFilesSizeInBytes)) return false;
      if (!Objects.equals(totalFilesSizeInBytes, that.totalFilesSizeInBytes)) return false;
      if (!Objects.equals(addedPositionalDeletes, that.addedPositionalDeletes)) return false;
      if (!Objects.equals(removedPositionalDeletes, that.removedPositionalDeletes)) return false;
      if (!Objects.equals(totalPositionalDeletes, that.totalPositionalDeletes)) return false;
      if (!Objects.equals(addedEqualityDeletes, that.addedEqualityDeletes)) return false;
      if (!Objects.equals(removedEqualityDeletes, that.removedEqualityDeletes)) return false;
      if (!Objects.equals(totalEqualityDeletes, that.totalEqualityDeletes)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(totalDuration, attempts, addedDataFiles, removedDataFiles, totalDataFiles, addedDeleteFiles, addedEqualityDeleteFiles, addedPositionalDeleteFiles, addedDVs, removedDeleteFiles, removedEqualityDeleteFiles, removedPositionalDeleteFiles, removedDVs, totalDeleteFiles, addedRecords, removedRecords, totalRecords, addedFilesSizeInBytes, removedFilesSizeInBytes, totalFilesSizeInBytes, addedPositionalDeletes, removedPositionalDeletes, totalPositionalDeletes, addedEqualityDeletes, removedEqualityDeletes, totalEqualityDeletes);
    }
  }

  /**
   * builder class for IcebergCommitReportOutputDatasetFacetCommitMetrics
   */
  public static final class IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder implements Builder<IcebergCommitReportOutputDatasetFacetCommitMetrics> {
    private Double totalDuration;

    private Double attempts;

    private Double addedDataFiles;

    private Double removedDataFiles;

    private Double totalDataFiles;

    private Double addedDeleteFiles;

    private Double addedEqualityDeleteFiles;

    private Double addedPositionalDeleteFiles;

    private Double addedDVs;

    private Double removedDeleteFiles;

    private Double removedEqualityDeleteFiles;

    private Double removedPositionalDeleteFiles;

    private Double removedDVs;

    private Double totalDeleteFiles;

    private Double addedRecords;

    private Double removedRecords;

    private Double totalRecords;

    private Double addedFilesSizeInBytes;

    private Double removedFilesSizeInBytes;

    private Double totalFilesSizeInBytes;

    private Double addedPositionalDeletes;

    private Double removedPositionalDeletes;

    private Double totalPositionalDeletes;

    private Double addedEqualityDeletes;

    private Double removedEqualityDeletes;

    private Double totalEqualityDeletes;

    /**
     * @param totalDuration Duration of the commit in MILLISECONDS
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder totalDuration(
        Double totalDuration) {
      this.totalDuration = totalDuration;
      return this;
    }

    /**
     * @param attempts Number of attempts made to commit the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder attempts(Double attempts) {
      this.attempts = attempts;
      return this;
    }

    /**
     * @param addedDataFiles Number of data files that are added during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedDataFiles(
        Double addedDataFiles) {
      this.addedDataFiles = addedDataFiles;
      return this;
    }

    /**
     * @param removedDataFiles Number of data files that are removed during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedDataFiles(
        Double removedDataFiles) {
      this.removedDataFiles = removedDataFiles;
      return this;
    }

    /**
     * @param totalDataFiles Total number of data files that are present in the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder totalDataFiles(
        Double totalDataFiles) {
      this.totalDataFiles = totalDataFiles;
      return this;
    }

    /**
     * @param addedDeleteFiles Number of delete files that are added during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedDeleteFiles(
        Double addedDeleteFiles) {
      this.addedDeleteFiles = addedDeleteFiles;
      return this;
    }

    /**
     * @param addedEqualityDeleteFiles Number of added equality delete files
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedEqualityDeleteFiles(
        Double addedEqualityDeleteFiles) {
      this.addedEqualityDeleteFiles = addedEqualityDeleteFiles;
      return this;
    }

    /**
     * @param addedPositionalDeleteFiles Number of added positional delete files
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedPositionalDeleteFiles(
        Double addedPositionalDeleteFiles) {
      this.addedPositionalDeleteFiles = addedPositionalDeleteFiles;
      return this;
    }

    /**
     * @param addedDVs Number of added DVs
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedDVs(Double addedDVs) {
      this.addedDVs = addedDVs;
      return this;
    }

    /**
     * @param removedDeleteFiles Number of delete files that are removed during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedDeleteFiles(
        Double removedDeleteFiles) {
      this.removedDeleteFiles = removedDeleteFiles;
      return this;
    }

    /**
     * @param removedEqualityDeleteFiles Number of removed equality delete files
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedEqualityDeleteFiles(
        Double removedEqualityDeleteFiles) {
      this.removedEqualityDeleteFiles = removedEqualityDeleteFiles;
      return this;
    }

    /**
     * @param removedPositionalDeleteFiles Number of removed positional delete files
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedPositionalDeleteFiles(
        Double removedPositionalDeleteFiles) {
      this.removedPositionalDeleteFiles = removedPositionalDeleteFiles;
      return this;
    }

    /**
     * @param removedDVs Number of removed DVs
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedDVs(Double removedDVs) {
      this.removedDVs = removedDVs;
      return this;
    }

    /**
     * @param totalDeleteFiles Total number of temporary delete files that are present in the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder totalDeleteFiles(
        Double totalDeleteFiles) {
      this.totalDeleteFiles = totalDeleteFiles;
      return this;
    }

    /**
     * @param addedRecords Number of records that are added during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedRecords(
        Double addedRecords) {
      this.addedRecords = addedRecords;
      return this;
    }

    /**
     * @param removedRecords Number of records that are removed during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedRecords(
        Double removedRecords) {
      this.removedRecords = removedRecords;
      return this;
    }

    /**
     * @param totalRecords Number of records that are present in the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder totalRecords(
        Double totalRecords) {
      this.totalRecords = totalRecords;
      return this;
    }

    /**
     * @param addedFilesSizeInBytes Number of files size in bytes that are added during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedFilesSizeInBytes(
        Double addedFilesSizeInBytes) {
      this.addedFilesSizeInBytes = addedFilesSizeInBytes;
      return this;
    }

    /**
     * @param removedFilesSizeInBytes Number of files size in bytes that are removed during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedFilesSizeInBytes(
        Double removedFilesSizeInBytes) {
      this.removedFilesSizeInBytes = removedFilesSizeInBytes;
      return this;
    }

    /**
     * @param totalFilesSizeInBytes Number of files size in bytes in the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder totalFilesSizeInBytes(
        Double totalFilesSizeInBytes) {
      this.totalFilesSizeInBytes = totalFilesSizeInBytes;
      return this;
    }

    /**
     * @param addedPositionalDeletes Number of positional deletes that are added during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedPositionalDeletes(
        Double addedPositionalDeletes) {
      this.addedPositionalDeletes = addedPositionalDeletes;
      return this;
    }

    /**
     * @param removedPositionalDeletes Number of positional deletes that are removed during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedPositionalDeletes(
        Double removedPositionalDeletes) {
      this.removedPositionalDeletes = removedPositionalDeletes;
      return this;
    }

    /**
     * @param totalPositionalDeletes Number of positional deletes that are present in the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder totalPositionalDeletes(
        Double totalPositionalDeletes) {
      this.totalPositionalDeletes = totalPositionalDeletes;
      return this;
    }

    /**
     * @param addedEqualityDeletes Number of equality deletes that are added during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder addedEqualityDeletes(
        Double addedEqualityDeletes) {
      this.addedEqualityDeletes = addedEqualityDeletes;
      return this;
    }

    /**
     * @param removedEqualityDeletes Number of equality deletes that are removed during the commit
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder removedEqualityDeletes(
        Double removedEqualityDeletes) {
      this.removedEqualityDeletes = removedEqualityDeletes;
      return this;
    }

    /**
     * @param totalEqualityDeletes Number of equality deletes that are present in the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetCommitMetricsBuilder totalEqualityDeletes(
        Double totalEqualityDeletes) {
      this.totalEqualityDeletes = totalEqualityDeletes;
      return this;
    }

    /**
     * build an instance of IcebergCommitReportOutputDatasetFacetCommitMetrics from the fields set in the builder
     */
    @Override
    public IcebergCommitReportOutputDatasetFacetCommitMetrics build() {
      IcebergCommitReportOutputDatasetFacetCommitMetrics __result = new IcebergCommitReportOutputDatasetFacetCommitMetrics(totalDuration, attempts, addedDataFiles, removedDataFiles, totalDataFiles, addedDeleteFiles, addedEqualityDeleteFiles, addedPositionalDeleteFiles, addedDVs, removedDeleteFiles, removedEqualityDeleteFiles, removedPositionalDeleteFiles, removedDVs, totalDeleteFiles, addedRecords, removedRecords, totalRecords, addedFilesSizeInBytes, removedFilesSizeInBytes, totalFilesSizeInBytes, addedPositionalDeletes, removedPositionalDeletes, totalPositionalDeletes, addedEqualityDeletes, removedEqualityDeletes, totalEqualityDeletes);
      return __result;
    }
  }

  public static class DefaultJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param _deleted set to true to delete a facet
     */
    @JsonCreator
    public DefaultJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("_deleted") Boolean _deleted) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobFacet");
      this._deleted = _deleted;
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
    @Override
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
  }

  /**
   * Interface for JobFacet
   */
  @JsonDeserialize(
      as = DefaultJobFacet.class
  )
  public interface JobFacet {
    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     */
    URI get_producer();

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet
     */
    URI get_schemaURL();

    /**
     * @return set to true to delete a facet
     */
    Boolean get_deleted();

    /**
     * @return set to true to delete a facet
     */
    @JsonIgnore
    default Boolean isDeleted() {
      return get_deleted();
    }

    /**
     * @return additional properties
     */
    Map<String, Object> getAdditionalProperties();
  }

  /**
   * model class for RunFacets
   */
  @JsonDeserialize(
      as = RunFacets.class
  )
  @JsonPropertyOrder({
      "externalQuery",
      "gcp_dataproc",
      "extractionError",
      "parent",
      "nominalTime",
      "tags",
      "errorMessage",
      "environmentVariables",
      "lineage",
      "test",
      "gcp_composer_run",
      "executionParameters",
      "jobDependencies",
      "processing_engine"
  })
  public static final class RunFacets {
    private final ExternalQueryRunFacet externalQuery;

    private final GcpDataprocRunFacet gcp_dataproc;

    private final ExtractionErrorRunFacet extractionError;

    private final ParentRunFacet parent;

    private final NominalTimeRunFacet nominalTime;

    private final TagsRunFacet tags;

    private final ErrorMessageRunFacet errorMessage;

    private final EnvironmentVariablesRunFacet environmentVariables;

    private final LineageRunFacet lineage;

    private final TestRunFacet test;

    private final GcpComposerRunFacet gcp_composer_run;

    private final ExecutionParametersRunFacet executionParameters;

    private final JobDependenciesRunFacet jobDependencies;

    private final ProcessingEngineRunFacet processing_engine;

    @JsonAnySetter
    private final Map<String, RunFacet> additionalProperties;

    /**
     * @param externalQuery the externalQuery
     * @param gcp_dataproc the gcp_dataproc
     * @param extractionError the extractionError
     * @param parent the parent
     * @param nominalTime the nominalTime
     * @param tags the tags
     * @param errorMessage the errorMessage
     * @param environmentVariables the environmentVariables
     * @param lineage the lineage
     * @param test the test
     * @param gcp_composer_run the gcp_composer_run
     * @param executionParameters the executionParameters
     * @param jobDependencies the jobDependencies
     * @param processing_engine the processing_engine
     */
    @JsonCreator
    private RunFacets(@JsonProperty("externalQuery") ExternalQueryRunFacet externalQuery,
        @JsonProperty("gcp_dataproc") GcpDataprocRunFacet gcp_dataproc,
        @JsonProperty("extractionError") ExtractionErrorRunFacet extractionError,
        @JsonProperty("parent") ParentRunFacet parent,
        @JsonProperty("nominalTime") NominalTimeRunFacet nominalTime,
        @JsonProperty("tags") TagsRunFacet tags,
        @JsonProperty("errorMessage") ErrorMessageRunFacet errorMessage,
        @JsonProperty("environmentVariables") EnvironmentVariablesRunFacet environmentVariables,
        @JsonProperty("lineage") LineageRunFacet lineage, @JsonProperty("test") TestRunFacet test,
        @JsonProperty("gcp_composer_run") GcpComposerRunFacet gcp_composer_run,
        @JsonProperty("executionParameters") ExecutionParametersRunFacet executionParameters,
        @JsonProperty("jobDependencies") JobDependenciesRunFacet jobDependencies,
        @JsonProperty("processing_engine") ProcessingEngineRunFacet processing_engine) {
      this.externalQuery = externalQuery;
      this.gcp_dataproc = gcp_dataproc;
      this.extractionError = extractionError;
      this.parent = parent;
      this.nominalTime = nominalTime;
      this.tags = tags;
      this.errorMessage = errorMessage;
      this.environmentVariables = environmentVariables;
      this.lineage = lineage;
      this.test = test;
      this.gcp_composer_run = gcp_composer_run;
      this.executionParameters = executionParameters;
      this.jobDependencies = jobDependencies;
      this.processing_engine = processing_engine;
      this.additionalProperties = new LinkedHashMap<>();
    }

    public ExternalQueryRunFacet getExternalQuery() {
      return externalQuery;
    }

    public GcpDataprocRunFacet getGcp_dataproc() {
      return gcp_dataproc;
    }

    public ExtractionErrorRunFacet getExtractionError() {
      return extractionError;
    }

    public ParentRunFacet getParent() {
      return parent;
    }

    public NominalTimeRunFacet getNominalTime() {
      return nominalTime;
    }

    public TagsRunFacet getTags() {
      return tags;
    }

    public ErrorMessageRunFacet getErrorMessage() {
      return errorMessage;
    }

    public EnvironmentVariablesRunFacet getEnvironmentVariables() {
      return environmentVariables;
    }

    public LineageRunFacet getLineage() {
      return lineage;
    }

    public TestRunFacet getTest() {
      return test;
    }

    public GcpComposerRunFacet getGcp_composer_run() {
      return gcp_composer_run;
    }

    public ExecutionParametersRunFacet getExecutionParameters() {
      return executionParameters;
    }

    public JobDependenciesRunFacet getJobDependencies() {
      return jobDependencies;
    }

    public ProcessingEngineRunFacet getProcessing_engine() {
      return processing_engine;
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
      if (!Objects.equals(externalQuery, that.externalQuery)) return false;
      if (!Objects.equals(gcp_dataproc, that.gcp_dataproc)) return false;
      if (!Objects.equals(extractionError, that.extractionError)) return false;
      if (!Objects.equals(parent, that.parent)) return false;
      if (!Objects.equals(nominalTime, that.nominalTime)) return false;
      if (!Objects.equals(tags, that.tags)) return false;
      if (!Objects.equals(errorMessage, that.errorMessage)) return false;
      if (!Objects.equals(environmentVariables, that.environmentVariables)) return false;
      if (!Objects.equals(lineage, that.lineage)) return false;
      if (!Objects.equals(test, that.test)) return false;
      if (!Objects.equals(gcp_composer_run, that.gcp_composer_run)) return false;
      if (!Objects.equals(executionParameters, that.executionParameters)) return false;
      if (!Objects.equals(jobDependencies, that.jobDependencies)) return false;
      if (!Objects.equals(processing_engine, that.processing_engine)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(externalQuery, gcp_dataproc, extractionError, parent, nominalTime, tags, errorMessage, environmentVariables, lineage, test, gcp_composer_run, executionParameters, jobDependencies, processing_engine, additionalProperties);
    }
  }

  /**
   * builder class for RunFacets
   */
  public static final class RunFacetsBuilder implements Builder<RunFacets> {
    private ExternalQueryRunFacet externalQuery;

    private GcpDataprocRunFacet gcp_dataproc;

    private ExtractionErrorRunFacet extractionError;

    private ParentRunFacet parent;

    private NominalTimeRunFacet nominalTime;

    private TagsRunFacet tags;

    private ErrorMessageRunFacet errorMessage;

    private EnvironmentVariablesRunFacet environmentVariables;

    private LineageRunFacet lineage;

    private TestRunFacet test;

    private GcpComposerRunFacet gcp_composer_run;

    private ExecutionParametersRunFacet executionParameters;

    private JobDependenciesRunFacet jobDependencies;

    private ProcessingEngineRunFacet processing_engine;

    private final Map<String, RunFacet> additionalProperties = new LinkedHashMap<>();

    /**
     * @param externalQuery the externalQuery
     * @return this
     */
    public RunFacetsBuilder externalQuery(ExternalQueryRunFacet externalQuery) {
      this.externalQuery = externalQuery;
      return this;
    }

    /**
     * @param gcp_dataproc the gcp_dataproc
     * @return this
     */
    public RunFacetsBuilder gcp_dataproc(GcpDataprocRunFacet gcp_dataproc) {
      this.gcp_dataproc = gcp_dataproc;
      return this;
    }

    /**
     * @param extractionError the extractionError
     * @return this
     */
    public RunFacetsBuilder extractionError(ExtractionErrorRunFacet extractionError) {
      this.extractionError = extractionError;
      return this;
    }

    /**
     * @param parent the parent
     * @return this
     */
    public RunFacetsBuilder parent(ParentRunFacet parent) {
      this.parent = parent;
      return this;
    }

    /**
     * @param nominalTime the nominalTime
     * @return this
     */
    public RunFacetsBuilder nominalTime(NominalTimeRunFacet nominalTime) {
      this.nominalTime = nominalTime;
      return this;
    }

    /**
     * @param tags the tags
     * @return this
     */
    public RunFacetsBuilder tags(TagsRunFacet tags) {
      this.tags = tags;
      return this;
    }

    /**
     * @param errorMessage the errorMessage
     * @return this
     */
    public RunFacetsBuilder errorMessage(ErrorMessageRunFacet errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    /**
     * @param environmentVariables the environmentVariables
     * @return this
     */
    public RunFacetsBuilder environmentVariables(
        EnvironmentVariablesRunFacet environmentVariables) {
      this.environmentVariables = environmentVariables;
      return this;
    }

    /**
     * @param lineage the lineage
     * @return this
     */
    public RunFacetsBuilder lineage(LineageRunFacet lineage) {
      this.lineage = lineage;
      return this;
    }

    /**
     * @param test the test
     * @return this
     */
    public RunFacetsBuilder test(TestRunFacet test) {
      this.test = test;
      return this;
    }

    /**
     * @param gcp_composer_run the gcp_composer_run
     * @return this
     */
    public RunFacetsBuilder gcp_composer_run(GcpComposerRunFacet gcp_composer_run) {
      this.gcp_composer_run = gcp_composer_run;
      return this;
    }

    /**
     * @param executionParameters the executionParameters
     * @return this
     */
    public RunFacetsBuilder executionParameters(ExecutionParametersRunFacet executionParameters) {
      this.executionParameters = executionParameters;
      return this;
    }

    /**
     * @param jobDependencies the jobDependencies
     * @return this
     */
    public RunFacetsBuilder jobDependencies(JobDependenciesRunFacet jobDependencies) {
      this.jobDependencies = jobDependencies;
      return this;
    }

    /**
     * @param processing_engine the processing_engine
     * @return this
     */
    public RunFacetsBuilder processing_engine(ProcessingEngineRunFacet processing_engine) {
      this.processing_engine = processing_engine;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public RunFacetsBuilder put(String key, RunFacet value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of RunFacets from the fields set in the builder
     */
    @Override
    public RunFacets build() {
      RunFacets __result = new RunFacets(externalQuery, gcp_dataproc, extractionError, parent, nominalTime, tags, errorMessage, environmentVariables, lineage, test, gcp_composer_run, executionParameters, jobDependencies, processing_engine);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for SchemaDatasetFacet
   */
  @JsonDeserialize(
      as = SchemaDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "fields"
  })
  public static final class SchemaDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<SchemaDatasetFacetFields> fields;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param fields The fields of the data source.
     */
    @JsonCreator
    private SchemaDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("fields") List<SchemaDatasetFacetFields> fields) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet");
      this._deleted = null;
      this.fields = fields;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The fields of the data source.
     */
    public List<SchemaDatasetFacetFields> getFields() {
      return fields;
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
      SchemaDatasetFacet that = (SchemaDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(fields, that.fields)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, fields, additionalProperties);
    }
  }

  /**
   * builder class for SchemaDatasetFacet
   */
  public final class SchemaDatasetFacetBuilder implements Builder<SchemaDatasetFacet> {
    private List<SchemaDatasetFacetFields> fields;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param fields The fields of the data source.
     * @return this
     */
    public SchemaDatasetFacetBuilder fields(List<SchemaDatasetFacetFields> fields) {
      this.fields = fields;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public SchemaDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of SchemaDatasetFacet from the fields set in the builder
     */
    @Override
    public SchemaDatasetFacet build() {
      SchemaDatasetFacet __result = new SchemaDatasetFacet(OpenLineage.this.producer, fields);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageJobInput
   */
  @JsonDeserialize(
      as = LineageJobInput.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name",
      "type",
      "field",
      "transformations"
  })
  public static final class LineageJobInput {
    private final String namespace;

    private final String name;

    private final String type;

    private final String field;

    private final List<LineageJobTransformation> transformations;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param namespace The namespace of the source entity.
     * @param name The name of the source entity.
     * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     * @param transformations Transformations applied to the source data.
     */
    @JsonCreator
    private LineageJobInput(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("type") String type,
        @JsonProperty("field") String field,
        @JsonProperty("transformations") List<LineageJobTransformation> transformations) {
      this.namespace = namespace;
      this.name = name;
      this.type = type;
      this.field = field;
      this.transformations = transformations;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The namespace of the source entity.
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The name of the source entity.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     */
    public String getType() {
      return type;
    }

    /**
     * @return The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     */
    public String getField() {
      return field;
    }

    /**
     * @return Transformations applied to the source data.
     */
    public List<LineageJobTransformation> getTransformations() {
      return transformations;
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
      LineageJobInput that = (LineageJobInput) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(field, that.field)) return false;
      if (!Objects.equals(transformations, that.transformations)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, type, field, transformations, additionalProperties);
    }
  }

  /**
   * builder class for LineageJobInput
   */
  public static final class LineageJobInputBuilder implements Builder<LineageJobInput> {
    private String namespace;

    private String name;

    private String type;

    private String field;

    private List<LineageJobTransformation> transformations;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param namespace The namespace of the source entity.
     * @return this
     */
    public LineageJobInputBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The name of the source entity.
     * @return this
     */
    public LineageJobInputBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param type The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job is the origin of data (e.g., a generator job that creates data without reading from any input dataset).
     * @return this
     */
    public LineageJobInputBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param field The specific field/column of the source dataset. When present at entity-level inputs, represents a dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the source column that feeds into the target column.
     * @return this
     */
    public LineageJobInputBuilder field(String field) {
      this.field = field;
      return this;
    }

    /**
     * @param transformations Transformations applied to the source data.
     * @return this
     */
    public LineageJobInputBuilder transformations(List<LineageJobTransformation> transformations) {
      this.transformations = transformations;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageJobInputBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageJobInput from the fields set in the builder
     */
    @Override
    public LineageJobInput build() {
      LineageJobInput __result = new LineageJobInput(namespace, name, type, field, transformations);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LocationSubsetCondition
   */
  @JsonDeserialize(
      as = LocationSubsetCondition.class
  )
  @JsonPropertyOrder({
      "type",
      "locations"
  })
  public static final class LocationSubsetCondition {
    private final String type;

    private final List<String> locations;

    /**
     * @param locations the locations
     */
    @JsonCreator
    private LocationSubsetCondition(@JsonProperty("locations") List<String> locations) {
      this.type = "location";
      this.locations = locations;
    }

    public String getType() {
      return type;
    }

    public List<String> getLocations() {
      return locations;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LocationSubsetCondition that = (LocationSubsetCondition) o;
      if (!Objects.equals(locations, that.locations)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(locations);
    }
  }

  /**
   * builder class for LocationSubsetCondition
   */
  public static final class LocationSubsetConditionBuilder implements Builder<LocationSubsetCondition> {
    private List<String> locations;

    /**
     * @param locations the locations
     * @return this
     */
    public LocationSubsetConditionBuilder locations(List<String> locations) {
      this.locations = locations;
      return this;
    }

    /**
     * build an instance of LocationSubsetCondition from the fields set in the builder
     */
    @Override
    public LocationSubsetCondition build() {
      LocationSubsetCondition __result = new LocationSubsetCondition(locations);
      return __result;
    }
  }

  /**
   * model class for SourceCodeJobFacet
   */
  @JsonDeserialize(
      as = SourceCodeJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "language",
      "sourceCode"
  })
  public static final class SourceCodeJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String language;

    private final String sourceCode;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param language Language in which source code of this job was written.
     * @param sourceCode Source code of this job.
     */
    @JsonCreator
    private SourceCodeJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("language") String language, @JsonProperty("sourceCode") String sourceCode) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/SourceCodeJobFacet.json#/$defs/SourceCodeJobFacet");
      this._deleted = null;
      this.language = language;
      this.sourceCode = sourceCode;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Language in which source code of this job was written.
     */
    public String getLanguage() {
      return language;
    }

    /**
     * @return Source code of this job.
     */
    public String getSourceCode() {
      return sourceCode;
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
      SourceCodeJobFacet that = (SourceCodeJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(language, that.language)) return false;
      if (!Objects.equals(sourceCode, that.sourceCode)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, language, sourceCode, additionalProperties);
    }
  }

  /**
   * builder class for SourceCodeJobFacet
   */
  public final class SourceCodeJobFacetBuilder implements Builder<SourceCodeJobFacet> {
    private String language;

    private String sourceCode;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param language Language in which source code of this job was written.
     * @return this
     */
    public SourceCodeJobFacetBuilder language(String language) {
      this.language = language;
      return this;
    }

    /**
     * @param sourceCode Source code of this job.
     * @return this
     */
    public SourceCodeJobFacetBuilder sourceCode(String sourceCode) {
      this.sourceCode = sourceCode;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public SourceCodeJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of SourceCodeJobFacet from the fields set in the builder
     */
    @Override
    public SourceCodeJobFacet build() {
      SourceCodeJobFacet __result = new SourceCodeJobFacet(OpenLineage.this.producer, language, sourceCode);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for IcebergScanReportInputDatasetFacetMetadata
   */
  @JsonDeserialize(
      as = IcebergScanReportInputDatasetFacetMetadata.class
  )
  @JsonPropertyOrder
  public static final class IcebergScanReportInputDatasetFacetMetadata {
    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    @JsonCreator
    private IcebergScanReportInputDatasetFacetMetadata() {
      this.additionalProperties = new LinkedHashMap<>();
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
      IcebergScanReportInputDatasetFacetMetadata that = (IcebergScanReportInputDatasetFacetMetadata) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for IcebergScanReportInputDatasetFacetMetadata
   */
  public static final class IcebergScanReportInputDatasetFacetMetadataBuilder implements Builder<IcebergScanReportInputDatasetFacetMetadata> {
    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public IcebergScanReportInputDatasetFacetMetadataBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of IcebergScanReportInputDatasetFacetMetadata from the fields set in the builder
     */
    @Override
    public IcebergScanReportInputDatasetFacetMetadata build() {
      IcebergScanReportInputDatasetFacetMetadata __result = new IcebergScanReportInputDatasetFacetMetadata();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ColumnLineageDatasetFacet
   */
  @JsonDeserialize(
      as = ColumnLineageDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "fields",
      "dataset"
  })
  public static final class ColumnLineageDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final ColumnLineageDatasetFacetFields fields;

    private final List<InputField> dataset;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param fields Column level lineage that maps output fields into input fields used to evaluate them.
     * @param dataset Column level lineage that affects the whole dataset. This includes filtering, sorting, grouping (aggregates), joining, window functions, etc.
     */
    @JsonCreator
    private ColumnLineageDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("fields") ColumnLineageDatasetFacetFields fields,
        @JsonProperty("dataset") List<InputField> dataset) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet");
      this._deleted = null;
      this.fields = fields;
      this.dataset = dataset;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Column level lineage that maps output fields into input fields used to evaluate them.
     */
    public ColumnLineageDatasetFacetFields getFields() {
      return fields;
    }

    /**
     * @return Column level lineage that affects the whole dataset. This includes filtering, sorting, grouping (aggregates), joining, window functions, etc.
     */
    public List<InputField> getDataset() {
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
      ColumnLineageDatasetFacet that = (ColumnLineageDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(fields, that.fields)) return false;
      if (!Objects.equals(dataset, that.dataset)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, fields, dataset, additionalProperties);
    }
  }

  /**
   * builder class for ColumnLineageDatasetFacet
   */
  public final class ColumnLineageDatasetFacetBuilder implements Builder<ColumnLineageDatasetFacet> {
    private ColumnLineageDatasetFacetFields fields;

    private List<InputField> dataset;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param fields Column level lineage that maps output fields into input fields used to evaluate them.
     * @return this
     */
    public ColumnLineageDatasetFacetBuilder fields(ColumnLineageDatasetFacetFields fields) {
      this.fields = fields;
      return this;
    }

    /**
     * @param dataset Column level lineage that affects the whole dataset. This includes filtering, sorting, grouping (aggregates), joining, window functions, etc.
     * @return this
     */
    public ColumnLineageDatasetFacetBuilder dataset(List<InputField> dataset) {
      this.dataset = dataset;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ColumnLineageDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ColumnLineageDatasetFacet from the fields set in the builder
     */
    @Override
    public ColumnLineageDatasetFacet build() {
      ColumnLineageDatasetFacet __result = new ColumnLineageDatasetFacet(OpenLineage.this.producer, fields, dataset);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DataQualityAssertionsDatasetFacet
   */
  @JsonDeserialize(
      as = DataQualityAssertionsDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "assertions"
  })
  public static final class DataQualityAssertionsDatasetFacet implements InputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final List<DataQualityAssertionsDatasetFacetAssertions> assertions;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param assertions the assertions
     */
    @JsonCreator
    private DataQualityAssertionsDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("assertions") List<DataQualityAssertionsDatasetFacetAssertions> assertions) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-2/DataQualityAssertionsDatasetFacet.json#/$defs/DataQualityAssertionsDatasetFacet");
      this.assertions = assertions;
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

    public List<DataQualityAssertionsDatasetFacetAssertions> getAssertions() {
      return assertions;
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
      DataQualityAssertionsDatasetFacet that = (DataQualityAssertionsDatasetFacet) o;
      if (!Objects.equals(assertions, that.assertions)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(assertions, additionalProperties);
    }
  }

  /**
   * builder class for DataQualityAssertionsDatasetFacet
   */
  public final class DataQualityAssertionsDatasetFacetBuilder implements Builder<DataQualityAssertionsDatasetFacet> {
    private List<DataQualityAssertionsDatasetFacetAssertions> assertions;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param assertions the assertions
     * @return this
     */
    public DataQualityAssertionsDatasetFacetBuilder assertions(
        List<DataQualityAssertionsDatasetFacetAssertions> assertions) {
      this.assertions = assertions;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DataQualityAssertionsDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DataQualityAssertionsDatasetFacet from the fields set in the builder
     */
    @Override
    public DataQualityAssertionsDatasetFacet build() {
      DataQualityAssertionsDatasetFacet __result = new DataQualityAssertionsDatasetFacet(OpenLineage.this.producer, assertions);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DatasetEvent
   */
  @JsonDeserialize(
      as = DatasetEvent.class
  )
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

    /**
     * @param eventTime the time the event occurred at
     * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param dataset the dataset
     */
    @JsonCreator
    private DatasetEvent(@JsonProperty("eventTime") ZonedDateTime eventTime,
        @JsonProperty("producer") URI producer, @JsonProperty("dataset") StaticDataset dataset) {
      this.eventTime = eventTime;
      this.producer = producer;
      this.schemaURL = URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent");
      this.dataset = dataset;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DatasetEvent that = (DatasetEvent) o;
      if (!Objects.equals(eventTime, that.eventTime)) return false;
      if (!Objects.equals(dataset, that.dataset)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(eventTime, dataset);
    }
  }

  /**
   * builder class for DatasetEvent
   */
  public final class DatasetEventBuilder implements Builder<DatasetEvent> {
    private ZonedDateTime eventTime;

    private StaticDataset dataset;

    /**
     * @param eventTime the time the event occurred at
     * @return this
     */
    public DatasetEventBuilder eventTime(ZonedDateTime eventTime) {
      this.eventTime = eventTime;
      return this;
    }

    /**
     * @param dataset the dataset
     * @return this
     */
    public DatasetEventBuilder dataset(StaticDataset dataset) {
      this.dataset = dataset;
      return this;
    }

    /**
     * build an instance of DatasetEvent from the fields set in the builder
     */
    @Override
    public DatasetEvent build() {
      DatasetEvent __result = new DatasetEvent(eventTime, OpenLineage.this.producer, dataset);
      return __result;
    }
  }

  /**
   * model class for TagsJobFacet
   */
  @JsonDeserialize(
      as = TagsJobFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "tags"
  })
  public static final class TagsJobFacet implements JobFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<TagsJobFacetFields> tags;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param tags The tags applied to the job facet
     */
    @JsonCreator
    private TagsJobFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("tags") List<TagsJobFacetFields> tags) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/TagsJobFacet.json#/$defs/TagsJobFacet");
      this._deleted = null;
      this.tags = tags;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return The tags applied to the job facet
     */
    public List<TagsJobFacetFields> getTags() {
      return tags;
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
      TagsJobFacet that = (TagsJobFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(tags, that.tags)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, tags, additionalProperties);
    }
  }

  /**
   * builder class for TagsJobFacet
   */
  public final class TagsJobFacetBuilder implements Builder<TagsJobFacet> {
    private List<TagsJobFacetFields> tags;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param tags The tags applied to the job facet
     * @return this
     */
    public TagsJobFacetBuilder tags(List<TagsJobFacetFields> tags) {
      this.tags = tags;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public TagsJobFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of TagsJobFacet from the fields set in the builder
     */
    @Override
    public TagsJobFacet build() {
      TagsJobFacet __result = new TagsJobFacet(OpenLineage.this.producer, tags);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageFieldEntry
   */
  @JsonDeserialize(
      as = LineageFieldEntry.class
  )
  @JsonPropertyOrder("inputs")
  public static final class LineageFieldEntry {
    private final List<LineageInput> inputs;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param inputs Source entities and/or fields that feed into this target field.
     */
    @JsonCreator
    private LineageFieldEntry(@JsonProperty("inputs") List<LineageInput> inputs) {
      this.inputs = inputs;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return Source entities and/or fields that feed into this target field.
     */
    public List<LineageInput> getInputs() {
      return inputs;
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
      LineageFieldEntry that = (LineageFieldEntry) o;
      if (!Objects.equals(inputs, that.inputs)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(inputs, additionalProperties);
    }
  }

  /**
   * builder class for LineageFieldEntry
   */
  public static final class LineageFieldEntryBuilder implements Builder<LineageFieldEntry> {
    private List<LineageInput> inputs;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param inputs Source entities and/or fields that feed into this target field.
     * @return this
     */
    public LineageFieldEntryBuilder inputs(List<LineageInput> inputs) {
      this.inputs = inputs;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageFieldEntryBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageFieldEntry from the fields set in the builder
     */
    @Override
    public LineageFieldEntry build() {
      LineageFieldEntry __result = new LineageFieldEntry(inputs);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ProcessingEngineRunFacet
   */
  @JsonDeserialize(
      as = ProcessingEngineRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "version",
      "name",
      "openlineageAdapterVersion"
  })
  public static final class ProcessingEngineRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final String version;

    private final String name;

    private final String openlineageAdapterVersion;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param version Processing engine version. Might be Airflow or Spark version.
     * @param name Processing engine name, e.g. Airflow or Spark
     * @param openlineageAdapterVersion OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration package version
     */
    @JsonCreator
    private ProcessingEngineRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("version") String version, @JsonProperty("name") String name,
        @JsonProperty("openlineageAdapterVersion") String openlineageAdapterVersion) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet");
      this.version = version;
      this.name = name;
      this.openlineageAdapterVersion = openlineageAdapterVersion;
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
     * @return Processing engine version. Might be Airflow or Spark version.
     */
    public String getVersion() {
      return version;
    }

    /**
     * @return Processing engine name, e.g. Airflow or Spark
     */
    public String getName() {
      return name;
    }

    /**
     * @return OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration package version
     */
    public String getOpenlineageAdapterVersion() {
      return openlineageAdapterVersion;
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
      ProcessingEngineRunFacet that = (ProcessingEngineRunFacet) o;
      if (!Objects.equals(version, that.version)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(openlineageAdapterVersion, that.openlineageAdapterVersion)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(version, name, openlineageAdapterVersion, additionalProperties);
    }
  }

  /**
   * builder class for ProcessingEngineRunFacet
   */
  public final class ProcessingEngineRunFacetBuilder implements Builder<ProcessingEngineRunFacet> {
    private String version;

    private String name;

    private String openlineageAdapterVersion;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param version Processing engine version. Might be Airflow or Spark version.
     * @return this
     */
    public ProcessingEngineRunFacetBuilder version(String version) {
      this.version = version;
      return this;
    }

    /**
     * @param name Processing engine name, e.g. Airflow or Spark
     * @return this
     */
    public ProcessingEngineRunFacetBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param openlineageAdapterVersion OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration package version
     * @return this
     */
    public ProcessingEngineRunFacetBuilder openlineageAdapterVersion(
        String openlineageAdapterVersion) {
      this.openlineageAdapterVersion = openlineageAdapterVersion;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ProcessingEngineRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ProcessingEngineRunFacet from the fields set in the builder
     */
    @Override
    public ProcessingEngineRunFacet build() {
      ProcessingEngineRunFacet __result = new ProcessingEngineRunFacet(OpenLineage.this.producer, version, name, openlineageAdapterVersion);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LifecycleStateChangeDatasetFacetPreviousIdentifier
   */
  @JsonDeserialize(
      as = LifecycleStateChangeDatasetFacetPreviousIdentifier.class
  )
  @JsonPropertyOrder({
      "name",
      "namespace"
  })
  public static final class LifecycleStateChangeDatasetFacetPreviousIdentifier {
    private final String name;

    private final String namespace;

    /**
     * @param name the name
     * @param namespace the namespace
     */
    @JsonCreator
    private LifecycleStateChangeDatasetFacetPreviousIdentifier(@JsonProperty("name") String name,
        @JsonProperty("namespace") String namespace) {
      this.name = name;
      this.namespace = namespace;
    }

    public String getName() {
      return name;
    }

    public String getNamespace() {
      return namespace;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LifecycleStateChangeDatasetFacetPreviousIdentifier that = (LifecycleStateChangeDatasetFacetPreviousIdentifier) o;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(namespace, that.namespace)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, namespace);
    }
  }

  /**
   * builder class for LifecycleStateChangeDatasetFacetPreviousIdentifier
   */
  public static final class LifecycleStateChangeDatasetFacetPreviousIdentifierBuilder implements Builder<LifecycleStateChangeDatasetFacetPreviousIdentifier> {
    private String name;

    private String namespace;

    /**
     * @param name the name
     * @return this
     */
    public LifecycleStateChangeDatasetFacetPreviousIdentifierBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param namespace the namespace
     * @return this
     */
    public LifecycleStateChangeDatasetFacetPreviousIdentifierBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * build an instance of LifecycleStateChangeDatasetFacetPreviousIdentifier from the fields set in the builder
     */
    @Override
    public LifecycleStateChangeDatasetFacetPreviousIdentifier build() {
      LifecycleStateChangeDatasetFacetPreviousIdentifier __result = new LifecycleStateChangeDatasetFacetPreviousIdentifier(name, namespace);
      return __result;
    }
  }

  /**
   * model class for InputFieldTransformations
   */
  @JsonDeserialize(
      as = InputFieldTransformations.class
  )
  @JsonPropertyOrder({
      "type",
      "subtype",
      "description",
      "masking"
  })
  public static final class InputFieldTransformations {
    private final String type;

    private final String subtype;

    private final String description;

    private final Boolean masking;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT
     * @param subtype The subtype of the transformation
     * @param description a string representation of the transformation applied
     * @param masking is transformation masking the data or not
     */
    @JsonCreator
    private InputFieldTransformations(@JsonProperty("type") String type,
        @JsonProperty("subtype") String subtype, @JsonProperty("description") String description,
        @JsonProperty("masking") Boolean masking) {
      this.type = type;
      this.subtype = subtype;
      this.description = description;
      this.masking = masking;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The type of the transformation. Allowed values are: DIRECT, INDIRECT
     */
    public String getType() {
      return type;
    }

    /**
     * @return The subtype of the transformation
     */
    public String getSubtype() {
      return subtype;
    }

    /**
     * @return a string representation of the transformation applied
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return is transformation masking the data or not
     */
    public Boolean getMasking() {
      return masking;
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
      InputFieldTransformations that = (InputFieldTransformations) o;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(subtype, that.subtype)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(masking, that.masking)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, subtype, description, masking, additionalProperties);
    }
  }

  /**
   * builder class for InputFieldTransformations
   */
  public static final class InputFieldTransformationsBuilder implements Builder<InputFieldTransformations> {
    private String type;

    private String subtype;

    private String description;

    private Boolean masking;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT
     * @return this
     */
    public InputFieldTransformationsBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param subtype The subtype of the transformation
     * @return this
     */
    public InputFieldTransformationsBuilder subtype(String subtype) {
      this.subtype = subtype;
      return this;
    }

    /**
     * @param description a string representation of the transformation applied
     * @return this
     */
    public InputFieldTransformationsBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param masking is transformation masking the data or not
     * @return this
     */
    public InputFieldTransformationsBuilder masking(Boolean masking) {
      this.masking = masking;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public InputFieldTransformationsBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of InputFieldTransformations from the fields set in the builder
     */
    @Override
    public InputFieldTransformations build() {
      InputFieldTransformations __result = new InputFieldTransformations(type, subtype, description, masking);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for LineageJobTransformation
   */
  @JsonDeserialize(
      as = LineageJobTransformation.class
  )
  @JsonPropertyOrder({
      "type",
      "subtype",
      "description",
      "masking"
  })
  public static final class LineageJobTransformation {
    private final String type;

    private final String subtype;

    private final String description;

    private final Boolean masking;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     * @param description A string representation of the transformation applied.
     * @param masking Whether the transformation masks the data (e.g., hashing PII).
     */
    @JsonCreator
    private LineageJobTransformation(@JsonProperty("type") String type,
        @JsonProperty("subtype") String subtype, @JsonProperty("description") String description,
        @JsonProperty("masking") Boolean masking) {
      this.type = type;
      this.subtype = subtype;
      this.description = description;
      this.masking = masking;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     */
    public String getType() {
      return type;
    }

    /**
     * @return The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     */
    public String getSubtype() {
      return subtype;
    }

    /**
     * @return A string representation of the transformation applied.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return Whether the transformation masks the data (e.g., hashing PII).
     */
    public Boolean getMasking() {
      return masking;
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
      LineageJobTransformation that = (LineageJobTransformation) o;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(subtype, that.subtype)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(masking, that.masking)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, subtype, description, masking, additionalProperties);
    }
  }

  /**
   * builder class for LineageJobTransformation
   */
  public static final class LineageJobTransformationBuilder implements Builder<LineageJobTransformation> {
    private String type;

    private String subtype;

    private String description;

    private Boolean masking;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param type The type of the transformation. Allowed values are: DIRECT, INDIRECT.
     * @return this
     */
    public LineageJobTransformationBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param subtype The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, CONDITIONAL.
     * @return this
     */
    public LineageJobTransformationBuilder subtype(String subtype) {
      this.subtype = subtype;
      return this;
    }

    /**
     * @param description A string representation of the transformation applied.
     * @return this
     */
    public LineageJobTransformationBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param masking Whether the transformation masks the data (e.g., hashing PII).
     * @return this
     */
    public LineageJobTransformationBuilder masking(Boolean masking) {
      this.masking = masking;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public LineageJobTransformationBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of LineageJobTransformation from the fields set in the builder
     */
    @Override
    public LineageJobTransformation build() {
      LineageJobTransformation __result = new LineageJobTransformation(type, subtype, description, masking);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for PartitionSubsetCondition
   */
  @JsonDeserialize(
      as = PartitionSubsetCondition.class
  )
  @JsonPropertyOrder({
      "type",
      "partitions"
  })
  public static final class PartitionSubsetCondition {
    private final String type;

    private final List<PartitionSubsetConditionPartitions> partitions;

    /**
     * @param partitions the partitions
     */
    @JsonCreator
    private PartitionSubsetCondition(
        @JsonProperty("partitions") List<PartitionSubsetConditionPartitions> partitions) {
      this.type = "partition";
      this.partitions = partitions;
    }

    public String getType() {
      return type;
    }

    public List<PartitionSubsetConditionPartitions> getPartitions() {
      return partitions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PartitionSubsetCondition that = (PartitionSubsetCondition) o;
      if (!Objects.equals(partitions, that.partitions)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(partitions);
    }
  }

  /**
   * builder class for PartitionSubsetCondition
   */
  public static final class PartitionSubsetConditionBuilder implements Builder<PartitionSubsetCondition> {
    private List<PartitionSubsetConditionPartitions> partitions;

    /**
     * @param partitions the partitions
     * @return this
     */
    public PartitionSubsetConditionBuilder partitions(
        List<PartitionSubsetConditionPartitions> partitions) {
      this.partitions = partitions;
      return this;
    }

    /**
     * build an instance of PartitionSubsetCondition from the fields set in the builder
     */
    @Override
    public PartitionSubsetCondition build() {
      PartitionSubsetCondition __result = new PartitionSubsetCondition(partitions);
      return __result;
    }
  }

  /**
   * model class for EnvironmentVariable
   */
  @JsonDeserialize(
      as = EnvironmentVariable.class
  )
  @JsonPropertyOrder({
      "name",
      "value"
  })
  public static final class EnvironmentVariable {
    private final String name;

    private final String value;

    /**
     * @param name The name of the environment variable.
     * @param value The value of the environment variable.
     */
    @JsonCreator
    private EnvironmentVariable(@JsonProperty("name") String name,
        @JsonProperty("value") String value) {
      this.name = name;
      this.value = value;
    }

    /**
     * @return The name of the environment variable.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The value of the environment variable.
     */
    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EnvironmentVariable that = (EnvironmentVariable) o;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(value, that.value)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }
  }

  /**
   * builder class for EnvironmentVariable
   */
  public static final class EnvironmentVariableBuilder implements Builder<EnvironmentVariable> {
    private String name;

    private String value;

    /**
     * @param name The name of the environment variable.
     * @return this
     */
    public EnvironmentVariableBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param value The value of the environment variable.
     * @return this
     */
    public EnvironmentVariableBuilder value(String value) {
      this.value = value;
      return this;
    }

    /**
     * build an instance of EnvironmentVariable from the fields set in the builder
     */
    @Override
    public EnvironmentVariable build() {
      EnvironmentVariable __result = new EnvironmentVariable(name, value);
      return __result;
    }
  }

  /**
   * model class for IcebergScanReportInputDatasetFacetScanMetrics
   */
  @JsonDeserialize(
      as = IcebergScanReportInputDatasetFacetScanMetrics.class
  )
  @JsonPropertyOrder({
      "totalPlanningDuration",
      "resultDataFiles",
      "resultDeleteFiles",
      "totalDataManifests",
      "totalDeleteManifests",
      "scannedDataManifests",
      "skippedDataManifests",
      "totalFileSizeInBytes",
      "totalDeleteFileSizeInBytes",
      "skippedDataFiles",
      "skippedDeleteFiles",
      "scannedDeleteManifests",
      "skippedDeleteManifests",
      "indexedDeleteFiles",
      "equalityDeleteFiles",
      "positionalDeleteFiles"
  })
  public static final class IcebergScanReportInputDatasetFacetScanMetrics {
    private final Double totalPlanningDuration;

    private final Double resultDataFiles;

    private final Double resultDeleteFiles;

    private final Double totalDataManifests;

    private final Double totalDeleteManifests;

    private final Double scannedDataManifests;

    private final Double skippedDataManifests;

    private final Double totalFileSizeInBytes;

    private final Double totalDeleteFileSizeInBytes;

    private final Double skippedDataFiles;

    private final Double skippedDeleteFiles;

    private final Double scannedDeleteManifests;

    private final Double skippedDeleteManifests;

    private final Double indexedDeleteFiles;

    private final Double equalityDeleteFiles;

    private final Double positionalDeleteFiles;

    /**
     * @param totalPlanningDuration Duration of the scan in MILLISECONDS
     * @param resultDataFiles List of data files that are read during the scan
     * @param resultDeleteFiles List of delete files that are read during the scan
     * @param totalDataManifests Total number of manifests that are scanned during the scan
     * @param totalDeleteManifests Total number of delete manifests that are scanned during the scan
     * @param scannedDataManifests Number of data manifests that are scanned during the scan
     * @param skippedDataManifests Number of data manifests that are skipped during the scan
     * @param totalFileSizeInBytes Total file size in bytes that are read during the scan
     * @param totalDeleteFileSizeInBytes Total delete file size in bytes that are read during the scan
     * @param skippedDataFiles Number of data files that are skipped during the scan
     * @param skippedDeleteFiles Number of delete files that are skipped during the scan
     * @param scannedDeleteManifests Number of delete manifests that are scanned during the scan
     * @param skippedDeleteManifests Number of delete manifests that are skipped during the scan
     * @param indexedDeleteFiles Number of delete files that are indexed during the scan
     * @param equalityDeleteFiles Number of delete files that are equality indexed during the scan
     * @param positionalDeleteFiles Number of delete files that are positional indexed during the scan
     */
    @JsonCreator
    private IcebergScanReportInputDatasetFacetScanMetrics(
        @JsonProperty("totalPlanningDuration") Double totalPlanningDuration,
        @JsonProperty("resultDataFiles") Double resultDataFiles,
        @JsonProperty("resultDeleteFiles") Double resultDeleteFiles,
        @JsonProperty("totalDataManifests") Double totalDataManifests,
        @JsonProperty("totalDeleteManifests") Double totalDeleteManifests,
        @JsonProperty("scannedDataManifests") Double scannedDataManifests,
        @JsonProperty("skippedDataManifests") Double skippedDataManifests,
        @JsonProperty("totalFileSizeInBytes") Double totalFileSizeInBytes,
        @JsonProperty("totalDeleteFileSizeInBytes") Double totalDeleteFileSizeInBytes,
        @JsonProperty("skippedDataFiles") Double skippedDataFiles,
        @JsonProperty("skippedDeleteFiles") Double skippedDeleteFiles,
        @JsonProperty("scannedDeleteManifests") Double scannedDeleteManifests,
        @JsonProperty("skippedDeleteManifests") Double skippedDeleteManifests,
        @JsonProperty("indexedDeleteFiles") Double indexedDeleteFiles,
        @JsonProperty("equalityDeleteFiles") Double equalityDeleteFiles,
        @JsonProperty("positionalDeleteFiles") Double positionalDeleteFiles) {
      this.totalPlanningDuration = totalPlanningDuration;
      this.resultDataFiles = resultDataFiles;
      this.resultDeleteFiles = resultDeleteFiles;
      this.totalDataManifests = totalDataManifests;
      this.totalDeleteManifests = totalDeleteManifests;
      this.scannedDataManifests = scannedDataManifests;
      this.skippedDataManifests = skippedDataManifests;
      this.totalFileSizeInBytes = totalFileSizeInBytes;
      this.totalDeleteFileSizeInBytes = totalDeleteFileSizeInBytes;
      this.skippedDataFiles = skippedDataFiles;
      this.skippedDeleteFiles = skippedDeleteFiles;
      this.scannedDeleteManifests = scannedDeleteManifests;
      this.skippedDeleteManifests = skippedDeleteManifests;
      this.indexedDeleteFiles = indexedDeleteFiles;
      this.equalityDeleteFiles = equalityDeleteFiles;
      this.positionalDeleteFiles = positionalDeleteFiles;
    }

    /**
     * @return Duration of the scan in MILLISECONDS
     */
    public Double getTotalPlanningDuration() {
      return totalPlanningDuration;
    }

    /**
     * @return List of data files that are read during the scan
     */
    public Double getResultDataFiles() {
      return resultDataFiles;
    }

    /**
     * @return List of delete files that are read during the scan
     */
    public Double getResultDeleteFiles() {
      return resultDeleteFiles;
    }

    /**
     * @return Total number of manifests that are scanned during the scan
     */
    public Double getTotalDataManifests() {
      return totalDataManifests;
    }

    /**
     * @return Total number of delete manifests that are scanned during the scan
     */
    public Double getTotalDeleteManifests() {
      return totalDeleteManifests;
    }

    /**
     * @return Number of data manifests that are scanned during the scan
     */
    public Double getScannedDataManifests() {
      return scannedDataManifests;
    }

    /**
     * @return Number of data manifests that are skipped during the scan
     */
    public Double getSkippedDataManifests() {
      return skippedDataManifests;
    }

    /**
     * @return Total file size in bytes that are read during the scan
     */
    public Double getTotalFileSizeInBytes() {
      return totalFileSizeInBytes;
    }

    /**
     * @return Total delete file size in bytes that are read during the scan
     */
    public Double getTotalDeleteFileSizeInBytes() {
      return totalDeleteFileSizeInBytes;
    }

    /**
     * @return Number of data files that are skipped during the scan
     */
    public Double getSkippedDataFiles() {
      return skippedDataFiles;
    }

    /**
     * @return Number of delete files that are skipped during the scan
     */
    public Double getSkippedDeleteFiles() {
      return skippedDeleteFiles;
    }

    /**
     * @return Number of delete manifests that are scanned during the scan
     */
    public Double getScannedDeleteManifests() {
      return scannedDeleteManifests;
    }

    /**
     * @return Number of delete manifests that are skipped during the scan
     */
    public Double getSkippedDeleteManifests() {
      return skippedDeleteManifests;
    }

    /**
     * @return Number of delete files that are indexed during the scan
     */
    public Double getIndexedDeleteFiles() {
      return indexedDeleteFiles;
    }

    /**
     * @return Number of delete files that are equality indexed during the scan
     */
    public Double getEqualityDeleteFiles() {
      return equalityDeleteFiles;
    }

    /**
     * @return Number of delete files that are positional indexed during the scan
     */
    public Double getPositionalDeleteFiles() {
      return positionalDeleteFiles;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IcebergScanReportInputDatasetFacetScanMetrics that = (IcebergScanReportInputDatasetFacetScanMetrics) o;
      if (!Objects.equals(totalPlanningDuration, that.totalPlanningDuration)) return false;
      if (!Objects.equals(resultDataFiles, that.resultDataFiles)) return false;
      if (!Objects.equals(resultDeleteFiles, that.resultDeleteFiles)) return false;
      if (!Objects.equals(totalDataManifests, that.totalDataManifests)) return false;
      if (!Objects.equals(totalDeleteManifests, that.totalDeleteManifests)) return false;
      if (!Objects.equals(scannedDataManifests, that.scannedDataManifests)) return false;
      if (!Objects.equals(skippedDataManifests, that.skippedDataManifests)) return false;
      if (!Objects.equals(totalFileSizeInBytes, that.totalFileSizeInBytes)) return false;
      if (!Objects.equals(totalDeleteFileSizeInBytes, that.totalDeleteFileSizeInBytes)) return false;
      if (!Objects.equals(skippedDataFiles, that.skippedDataFiles)) return false;
      if (!Objects.equals(skippedDeleteFiles, that.skippedDeleteFiles)) return false;
      if (!Objects.equals(scannedDeleteManifests, that.scannedDeleteManifests)) return false;
      if (!Objects.equals(skippedDeleteManifests, that.skippedDeleteManifests)) return false;
      if (!Objects.equals(indexedDeleteFiles, that.indexedDeleteFiles)) return false;
      if (!Objects.equals(equalityDeleteFiles, that.equalityDeleteFiles)) return false;
      if (!Objects.equals(positionalDeleteFiles, that.positionalDeleteFiles)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(totalPlanningDuration, resultDataFiles, resultDeleteFiles, totalDataManifests, totalDeleteManifests, scannedDataManifests, skippedDataManifests, totalFileSizeInBytes, totalDeleteFileSizeInBytes, skippedDataFiles, skippedDeleteFiles, scannedDeleteManifests, skippedDeleteManifests, indexedDeleteFiles, equalityDeleteFiles, positionalDeleteFiles);
    }
  }

  /**
   * builder class for IcebergScanReportInputDatasetFacetScanMetrics
   */
  public static final class IcebergScanReportInputDatasetFacetScanMetricsBuilder implements Builder<IcebergScanReportInputDatasetFacetScanMetrics> {
    private Double totalPlanningDuration;

    private Double resultDataFiles;

    private Double resultDeleteFiles;

    private Double totalDataManifests;

    private Double totalDeleteManifests;

    private Double scannedDataManifests;

    private Double skippedDataManifests;

    private Double totalFileSizeInBytes;

    private Double totalDeleteFileSizeInBytes;

    private Double skippedDataFiles;

    private Double skippedDeleteFiles;

    private Double scannedDeleteManifests;

    private Double skippedDeleteManifests;

    private Double indexedDeleteFiles;

    private Double equalityDeleteFiles;

    private Double positionalDeleteFiles;

    /**
     * @param totalPlanningDuration Duration of the scan in MILLISECONDS
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder totalPlanningDuration(
        Double totalPlanningDuration) {
      this.totalPlanningDuration = totalPlanningDuration;
      return this;
    }

    /**
     * @param resultDataFiles List of data files that are read during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder resultDataFiles(
        Double resultDataFiles) {
      this.resultDataFiles = resultDataFiles;
      return this;
    }

    /**
     * @param resultDeleteFiles List of delete files that are read during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder resultDeleteFiles(
        Double resultDeleteFiles) {
      this.resultDeleteFiles = resultDeleteFiles;
      return this;
    }

    /**
     * @param totalDataManifests Total number of manifests that are scanned during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder totalDataManifests(
        Double totalDataManifests) {
      this.totalDataManifests = totalDataManifests;
      return this;
    }

    /**
     * @param totalDeleteManifests Total number of delete manifests that are scanned during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder totalDeleteManifests(
        Double totalDeleteManifests) {
      this.totalDeleteManifests = totalDeleteManifests;
      return this;
    }

    /**
     * @param scannedDataManifests Number of data manifests that are scanned during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder scannedDataManifests(
        Double scannedDataManifests) {
      this.scannedDataManifests = scannedDataManifests;
      return this;
    }

    /**
     * @param skippedDataManifests Number of data manifests that are skipped during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder skippedDataManifests(
        Double skippedDataManifests) {
      this.skippedDataManifests = skippedDataManifests;
      return this;
    }

    /**
     * @param totalFileSizeInBytes Total file size in bytes that are read during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder totalFileSizeInBytes(
        Double totalFileSizeInBytes) {
      this.totalFileSizeInBytes = totalFileSizeInBytes;
      return this;
    }

    /**
     * @param totalDeleteFileSizeInBytes Total delete file size in bytes that are read during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder totalDeleteFileSizeInBytes(
        Double totalDeleteFileSizeInBytes) {
      this.totalDeleteFileSizeInBytes = totalDeleteFileSizeInBytes;
      return this;
    }

    /**
     * @param skippedDataFiles Number of data files that are skipped during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder skippedDataFiles(
        Double skippedDataFiles) {
      this.skippedDataFiles = skippedDataFiles;
      return this;
    }

    /**
     * @param skippedDeleteFiles Number of delete files that are skipped during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder skippedDeleteFiles(
        Double skippedDeleteFiles) {
      this.skippedDeleteFiles = skippedDeleteFiles;
      return this;
    }

    /**
     * @param scannedDeleteManifests Number of delete manifests that are scanned during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder scannedDeleteManifests(
        Double scannedDeleteManifests) {
      this.scannedDeleteManifests = scannedDeleteManifests;
      return this;
    }

    /**
     * @param skippedDeleteManifests Number of delete manifests that are skipped during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder skippedDeleteManifests(
        Double skippedDeleteManifests) {
      this.skippedDeleteManifests = skippedDeleteManifests;
      return this;
    }

    /**
     * @param indexedDeleteFiles Number of delete files that are indexed during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder indexedDeleteFiles(
        Double indexedDeleteFiles) {
      this.indexedDeleteFiles = indexedDeleteFiles;
      return this;
    }

    /**
     * @param equalityDeleteFiles Number of delete files that are equality indexed during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder equalityDeleteFiles(
        Double equalityDeleteFiles) {
      this.equalityDeleteFiles = equalityDeleteFiles;
      return this;
    }

    /**
     * @param positionalDeleteFiles Number of delete files that are positional indexed during the scan
     * @return this
     */
    public IcebergScanReportInputDatasetFacetScanMetricsBuilder positionalDeleteFiles(
        Double positionalDeleteFiles) {
      this.positionalDeleteFiles = positionalDeleteFiles;
      return this;
    }

    /**
     * build an instance of IcebergScanReportInputDatasetFacetScanMetrics from the fields set in the builder
     */
    @Override
    public IcebergScanReportInputDatasetFacetScanMetrics build() {
      IcebergScanReportInputDatasetFacetScanMetrics __result = new IcebergScanReportInputDatasetFacetScanMetrics(totalPlanningDuration, resultDataFiles, resultDeleteFiles, totalDataManifests, totalDeleteManifests, scannedDataManifests, skippedDataManifests, totalFileSizeInBytes, totalDeleteFileSizeInBytes, skippedDataFiles, skippedDeleteFiles, scannedDeleteManifests, skippedDeleteManifests, indexedDeleteFiles, equalityDeleteFiles, positionalDeleteFiles);
      return __result;
    }
  }

  /**
   * model class for TestExecution
   */
  @JsonDeserialize(
      as = TestExecution.class
  )
  @JsonPropertyOrder({
      "name",
      "status",
      "severity",
      "type",
      "description",
      "expected",
      "actual",
      "content",
      "contentType",
      "params"
  })
  public static final class TestExecution {
    private final String name;

    private final String status;

    private final String severity;

    private final String type;

    private final String description;

    private final String expected;

    private final String actual;

    private final String content;

    private final String contentType;

    private final TestExecutionParams params;

    /**
     * @param name Name identifying the test.
     * @param status Whether the test found issues: 'pass' (no issues found), 'fail' (issues found), 'skip' (not executed). Independent of severity — a test can fail without blocking the pipeline when severity is 'warn'.
     * @param severity The configured consequence of a test failure: 'error' (blocks pipeline execution) or 'warn' (produces a warning only, does not block). A test with severity 'warn' and status 'fail' means issues were found but execution continued.
     * @param type Classification of the test, e.g. 'not_null', 'unique', 'row_count', 'freshness', 'custom_sql'.
     * @param description Human-readable description of what the test checks.
     * @param expected The expected value or threshold for the test, serialized as a string.
     * @param actual The actual value observed during the test, serialized as a string.
     * @param content The test body, e.g. a SQL query or expression.
     * @param contentType The format of the content field, allowing consumers to interpret or filter test content. Common values include 'sql', 'json', 'expression'.
     * @param params Arbitrary key-value pairs for check-specific inputs.
     */
    @JsonCreator
    private TestExecution(@JsonProperty("name") String name, @JsonProperty("status") String status,
        @JsonProperty("severity") String severity, @JsonProperty("type") String type,
        @JsonProperty("description") String description, @JsonProperty("expected") String expected,
        @JsonProperty("actual") String actual, @JsonProperty("content") String content,
        @JsonProperty("contentType") String contentType,
        @JsonProperty("params") TestExecutionParams params) {
      this.name = name;
      this.status = status;
      this.severity = severity;
      this.type = type;
      this.description = description;
      this.expected = expected;
      this.actual = actual;
      this.content = content;
      this.contentType = contentType;
      this.params = params;
    }

    /**
     * @return Name identifying the test.
     */
    public String getName() {
      return name;
    }

    /**
     * @return Whether the test found issues: 'pass' (no issues found), 'fail' (issues found), 'skip' (not executed). Independent of severity — a test can fail without blocking the pipeline when severity is 'warn'.
     */
    public String getStatus() {
      return status;
    }

    /**
     * @return The configured consequence of a test failure: 'error' (blocks pipeline execution) or 'warn' (produces a warning only, does not block). A test with severity 'warn' and status 'fail' means issues were found but execution continued.
     */
    public String getSeverity() {
      return severity;
    }

    /**
     * @return Classification of the test, e.g. 'not_null', 'unique', 'row_count', 'freshness', 'custom_sql'.
     */
    public String getType() {
      return type;
    }

    /**
     * @return Human-readable description of what the test checks.
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return The expected value or threshold for the test, serialized as a string.
     */
    public String getExpected() {
      return expected;
    }

    /**
     * @return The actual value observed during the test, serialized as a string.
     */
    public String getActual() {
      return actual;
    }

    /**
     * @return The test body, e.g. a SQL query or expression.
     */
    public String getContent() {
      return content;
    }

    /**
     * @return The format of the content field, allowing consumers to interpret or filter test content. Common values include 'sql', 'json', 'expression'.
     */
    public String getContentType() {
      return contentType;
    }

    /**
     * @return Arbitrary key-value pairs for check-specific inputs.
     */
    public TestExecutionParams getParams() {
      return params;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestExecution that = (TestExecution) o;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(status, that.status)) return false;
      if (!Objects.equals(severity, that.severity)) return false;
      if (!Objects.equals(type, that.type)) return false;
      if (!Objects.equals(description, that.description)) return false;
      if (!Objects.equals(expected, that.expected)) return false;
      if (!Objects.equals(actual, that.actual)) return false;
      if (!Objects.equals(content, that.content)) return false;
      if (!Objects.equals(contentType, that.contentType)) return false;
      if (!Objects.equals(params, that.params)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, status, severity, type, description, expected, actual, content, contentType, params);
    }
  }

  /**
   * builder class for TestExecution
   */
  public static final class TestExecutionBuilder implements Builder<TestExecution> {
    private String name;

    private String status;

    private String severity;

    private String type;

    private String description;

    private String expected;

    private String actual;

    private String content;

    private String contentType;

    private TestExecutionParams params;

    /**
     * @param name Name identifying the test.
     * @return this
     */
    public TestExecutionBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param status Whether the test found issues: 'pass' (no issues found), 'fail' (issues found), 'skip' (not executed). Independent of severity — a test can fail without blocking the pipeline when severity is 'warn'.
     * @return this
     */
    public TestExecutionBuilder status(String status) {
      this.status = status;
      return this;
    }

    /**
     * @param severity The configured consequence of a test failure: 'error' (blocks pipeline execution) or 'warn' (produces a warning only, does not block). A test with severity 'warn' and status 'fail' means issues were found but execution continued.
     * @return this
     */
    public TestExecutionBuilder severity(String severity) {
      this.severity = severity;
      return this;
    }

    /**
     * @param type Classification of the test, e.g. 'not_null', 'unique', 'row_count', 'freshness', 'custom_sql'.
     * @return this
     */
    public TestExecutionBuilder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * @param description Human-readable description of what the test checks.
     * @return this
     */
    public TestExecutionBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * @param expected The expected value or threshold for the test, serialized as a string.
     * @return this
     */
    public TestExecutionBuilder expected(String expected) {
      this.expected = expected;
      return this;
    }

    /**
     * @param actual The actual value observed during the test, serialized as a string.
     * @return this
     */
    public TestExecutionBuilder actual(String actual) {
      this.actual = actual;
      return this;
    }

    /**
     * @param content The test body, e.g. a SQL query or expression.
     * @return this
     */
    public TestExecutionBuilder content(String content) {
      this.content = content;
      return this;
    }

    /**
     * @param contentType The format of the content field, allowing consumers to interpret or filter test content. Common values include 'sql', 'json', 'expression'.
     * @return this
     */
    public TestExecutionBuilder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    /**
     * @param params Arbitrary key-value pairs for check-specific inputs.
     * @return this
     */
    public TestExecutionBuilder params(TestExecutionParams params) {
      this.params = params;
      return this;
    }

    /**
     * build an instance of TestExecution from the fields set in the builder
     */
    @Override
    public TestExecution build() {
      TestExecution __result = new TestExecution(name, status, severity, type, description, expected, actual, content, contentType, params);
      return __result;
    }
  }

  /**
   * model class for TagsRunFacetFields
   */
  @JsonDeserialize(
      as = TagsRunFacetFields.class
  )
  @JsonPropertyOrder({
      "key",
      "value",
      "source"
  })
  public static final class TagsRunFacetFields {
    private final String key;

    private final String value;

    private final String source;

    /**
     * @param key Key that identifies the tag
     * @param value The value of the field
     * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     */
    @JsonCreator
    private TagsRunFacetFields(@JsonProperty("key") String key, @JsonProperty("value") String value,
        @JsonProperty("source") String source) {
      this.key = key;
      this.value = value;
      this.source = source;
    }

    /**
     * @return Key that identifies the tag
     */
    public String getKey() {
      return key;
    }

    /**
     * @return The value of the field
     */
    public String getValue() {
      return value;
    }

    /**
     * @return The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     */
    public String getSource() {
      return source;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TagsRunFacetFields that = (TagsRunFacetFields) o;
      if (!Objects.equals(key, that.key)) return false;
      if (!Objects.equals(value, that.value)) return false;
      if (!Objects.equals(source, that.source)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, source);
    }
  }

  /**
   * builder class for TagsRunFacetFields
   */
  public static final class TagsRunFacetFieldsBuilder implements Builder<TagsRunFacetFields> {
    private String key;

    private String value;

    private String source;

    /**
     * @param key Key that identifies the tag
     * @return this
     */
    public TagsRunFacetFieldsBuilder key(String key) {
      this.key = key;
      return this;
    }

    /**
     * @param value The value of the field
     * @return this
     */
    public TagsRunFacetFieldsBuilder value(String value) {
      this.value = value;
      return this;
    }

    /**
     * @param source The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
     * @return this
     */
    public TagsRunFacetFieldsBuilder source(String source) {
      this.source = source;
      return this;
    }

    /**
     * build an instance of TagsRunFacetFields from the fields set in the builder
     */
    @Override
    public TagsRunFacetFields build() {
      TagsRunFacetFields __result = new TagsRunFacetFields(key, value, source);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsDatasetFacetColumnMetricsAdditional
   */
  @JsonDeserialize(
      as = DataQualityMetricsDatasetFacetColumnMetricsAdditional.class
  )
  @JsonPropertyOrder({
      "nullCount",
      "distinctCount",
      "sum",
      "count",
      "min",
      "max",
      "quantiles"
  })
  public static final class DataQualityMetricsDatasetFacetColumnMetricsAdditional {
    private final Long nullCount;

    private final Long distinctCount;

    private final Double sum;

    private final Double count;

    private final Double min;

    private final Double max;

    private final DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles quantiles;

    /**
     * @param nullCount The number of null values in this column for the rows evaluated
     * @param distinctCount The number of distinct values in this column for the rows evaluated
     * @param sum The total sum of values in this column for the rows evaluated
     * @param count The number of values in this column
     * @param min the min
     * @param max the max
     * @param quantiles The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
     */
    @JsonCreator
    private DataQualityMetricsDatasetFacetColumnMetricsAdditional(
        @JsonProperty("nullCount") Long nullCount,
        @JsonProperty("distinctCount") Long distinctCount, @JsonProperty("sum") Double sum,
        @JsonProperty("count") Double count, @JsonProperty("min") Double min,
        @JsonProperty("max") Double max,
        @JsonProperty("quantiles") DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles quantiles) {
      this.nullCount = nullCount;
      this.distinctCount = distinctCount;
      this.sum = sum;
      this.count = count;
      this.min = min;
      this.max = max;
      this.quantiles = quantiles;
    }

    /**
     * @return The number of null values in this column for the rows evaluated
     */
    public Long getNullCount() {
      return nullCount;
    }

    /**
     * @return The number of distinct values in this column for the rows evaluated
     */
    public Long getDistinctCount() {
      return distinctCount;
    }

    /**
     * @return The total sum of values in this column for the rows evaluated
     */
    public Double getSum() {
      return sum;
    }

    /**
     * @return The number of values in this column
     */
    public Double getCount() {
      return count;
    }

    public Double getMin() {
      return min;
    }

    public Double getMax() {
      return max;
    }

    /**
     * @return The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles getQuantiles() {
      return quantiles;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataQualityMetricsDatasetFacetColumnMetricsAdditional that = (DataQualityMetricsDatasetFacetColumnMetricsAdditional) o;
      if (!Objects.equals(nullCount, that.nullCount)) return false;
      if (!Objects.equals(distinctCount, that.distinctCount)) return false;
      if (!Objects.equals(sum, that.sum)) return false;
      if (!Objects.equals(count, that.count)) return false;
      if (!Objects.equals(min, that.min)) return false;
      if (!Objects.equals(max, that.max)) return false;
      if (!Objects.equals(quantiles, that.quantiles)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(nullCount, distinctCount, sum, count, min, max, quantiles);
    }
  }

  /**
   * builder class for DataQualityMetricsDatasetFacetColumnMetricsAdditional
   */
  public static final class DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder implements Builder<DataQualityMetricsDatasetFacetColumnMetricsAdditional> {
    private Long nullCount;

    private Long distinctCount;

    private Double sum;

    private Double count;

    private Double min;

    private Double max;

    private DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles quantiles;

    /**
     * @param nullCount The number of null values in this column for the rows evaluated
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder nullCount(Long nullCount) {
      this.nullCount = nullCount;
      return this;
    }

    /**
     * @param distinctCount The number of distinct values in this column for the rows evaluated
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder distinctCount(
        Long distinctCount) {
      this.distinctCount = distinctCount;
      return this;
    }

    /**
     * @param sum The total sum of values in this column for the rows evaluated
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder sum(Double sum) {
      this.sum = sum;
      return this;
    }

    /**
     * @param count The number of values in this column
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder count(Double count) {
      this.count = count;
      return this;
    }

    /**
     * @param min the min
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder min(Double min) {
      this.min = min;
      return this;
    }

    /**
     * @param max the max
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder max(Double max) {
      this.max = max;
      return this;
    }

    /**
     * @param quantiles The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder quantiles(
        DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles quantiles) {
      this.quantiles = quantiles;
      return this;
    }

    /**
     * build an instance of DataQualityMetricsDatasetFacetColumnMetricsAdditional from the fields set in the builder
     */
    @Override
    public DataQualityMetricsDatasetFacetColumnMetricsAdditional build() {
      DataQualityMetricsDatasetFacetColumnMetricsAdditional __result = new DataQualityMetricsDatasetFacetColumnMetricsAdditional(nullCount, distinctCount, sum, count, min, max, quantiles);
      return __result;
    }
  }

  /**
   * model class for DataQualityAssertionsDatasetFacetAssertions
   */
  @JsonDeserialize(
      as = DataQualityAssertionsDatasetFacetAssertions.class
  )
  @JsonPropertyOrder({
      "assertion",
      "success",
      "column",
      "severity"
  })
  public static final class DataQualityAssertionsDatasetFacetAssertions {
    private final String assertion;

    private final Boolean success;

    private final String column;

    private final String severity;

    /**
     * @param assertion Type of expectation test that dataset is subjected to
     * @param success the success
     * @param column Column that expectation is testing. It should match the name provided in SchemaDatasetFacet. If column field is empty, then expectation refers to whole dataset.
     * @param severity The configured severity level of the assertion. Common values are 'error' (test failure blocks pipeline) or 'warn' (test failure produces warning only).
     */
    @JsonCreator
    private DataQualityAssertionsDatasetFacetAssertions(@JsonProperty("assertion") String assertion,
        @JsonProperty("success") Boolean success, @JsonProperty("column") String column,
        @JsonProperty("severity") String severity) {
      this.assertion = assertion;
      this.success = success;
      this.column = column;
      this.severity = severity;
    }

    /**
     * @return Type of expectation test that dataset is subjected to
     */
    public String getAssertion() {
      return assertion;
    }

    public Boolean getSuccess() {
      return success;
    }

    /**
     * @return Column that expectation is testing. It should match the name provided in SchemaDatasetFacet. If column field is empty, then expectation refers to whole dataset.
     */
    public String getColumn() {
      return column;
    }

    /**
     * @return The configured severity level of the assertion. Common values are 'error' (test failure blocks pipeline) or 'warn' (test failure produces warning only).
     */
    public String getSeverity() {
      return severity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataQualityAssertionsDatasetFacetAssertions that = (DataQualityAssertionsDatasetFacetAssertions) o;
      if (!Objects.equals(assertion, that.assertion)) return false;
      if (!Objects.equals(success, that.success)) return false;
      if (!Objects.equals(column, that.column)) return false;
      if (!Objects.equals(severity, that.severity)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(assertion, success, column, severity);
    }
  }

  /**
   * builder class for DataQualityAssertionsDatasetFacetAssertions
   */
  public static final class DataQualityAssertionsDatasetFacetAssertionsBuilder implements Builder<DataQualityAssertionsDatasetFacetAssertions> {
    private String assertion;

    private Boolean success;

    private String column;

    private String severity;

    /**
     * @param assertion Type of expectation test that dataset is subjected to
     * @return this
     */
    public DataQualityAssertionsDatasetFacetAssertionsBuilder assertion(String assertion) {
      this.assertion = assertion;
      return this;
    }

    /**
     * @param success the success
     * @return this
     */
    public DataQualityAssertionsDatasetFacetAssertionsBuilder success(Boolean success) {
      this.success = success;
      return this;
    }

    /**
     * @param column Column that expectation is testing. It should match the name provided in SchemaDatasetFacet. If column field is empty, then expectation refers to whole dataset.
     * @return this
     */
    public DataQualityAssertionsDatasetFacetAssertionsBuilder column(String column) {
      this.column = column;
      return this;
    }

    /**
     * @param severity The configured severity level of the assertion. Common values are 'error' (test failure blocks pipeline) or 'warn' (test failure produces warning only).
     * @return this
     */
    public DataQualityAssertionsDatasetFacetAssertionsBuilder severity(String severity) {
      this.severity = severity;
      return this;
    }

    /**
     * build an instance of DataQualityAssertionsDatasetFacetAssertions from the fields set in the builder
     */
    @Override
    public DataQualityAssertionsDatasetFacetAssertions build() {
      DataQualityAssertionsDatasetFacetAssertions __result = new DataQualityAssertionsDatasetFacetAssertions(assertion, success, column, severity);
      return __result;
    }
  }

  /**
   * model class for ParentRunFacetRun
   */
  @JsonDeserialize(
      as = ParentRunFacetRun.class
  )
  @JsonPropertyOrder("runId")
  public static final class ParentRunFacetRun {
    private final UUID runId;

    /**
     * @param runId The globally unique ID of the run associated with the job.
     */
    @JsonCreator
    private ParentRunFacetRun(@JsonProperty("runId") UUID runId) {
      this.runId = runId;
    }

    /**
     * @return The globally unique ID of the run associated with the job.
     */
    public UUID getRunId() {
      return runId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ParentRunFacetRun that = (ParentRunFacetRun) o;
      if (!Objects.equals(runId, that.runId)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(runId);
    }
  }

  /**
   * builder class for ParentRunFacetRun
   */
  public static final class ParentRunFacetRunBuilder implements Builder<ParentRunFacetRun> {
    private UUID runId;

    /**
     * @param runId The globally unique ID of the run associated with the job.
     * @return this
     */
    public ParentRunFacetRunBuilder runId(UUID runId) {
      this.runId = runId;
      return this;
    }

    /**
     * build an instance of ParentRunFacetRun from the fields set in the builder
     */
    @Override
    public ParentRunFacetRun build() {
      ParentRunFacetRun __result = new ParentRunFacetRun(runId);
      return __result;
    }
  }

  /**
   * model class for GcpDataprocRunFacet
   */
  @JsonDeserialize(
      as = GcpDataprocRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "appId",
      "appName",
      "batchId",
      "batchUuid",
      "clusterName",
      "clusterUuid",
      "jobId",
      "jobUuid",
      "projectId",
      "queryNodeName",
      "jobType",
      "sessionId",
      "sessionUuid"
  })
  public static final class GcpDataprocRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final String appId;

    private final String appName;

    private final String batchId;

    private final String batchUuid;

    private final String clusterName;

    private final String clusterUuid;

    private final String jobId;

    private final String jobUuid;

    private final String projectId;

    private final String queryNodeName;

    private final String jobType;

    private final String sessionId;

    private final String sessionUuid;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param appId Application ID set by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
     * @param appName App name which may be provided by the user, or some default is used by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
     * @param batchId Populated only for Dataproc serverless batches. The resource id of the batch.
     * @param batchUuid Populated only for Dataproc serverless batches. A UUID generated by the service when it creates the batch.
     * @param clusterName Populated only for Dataproc GCE workloads. The cluster name is unique within a GCP project.
     * @param clusterUuid Populated only for Dataproc GCE workloads. A UUID generated by the service at the time of cluster creation.
     * @param jobId Populated only for Dataproc GCE workloads. If not specified by the user, the job ID will be provided by the service.
     * @param jobUuid Populated only for Dataproc GCE workloads. A UUID that uniquely identifies a job within the project over time.
     * @param projectId The GCP project ID that the resource belongs to.
     * @param queryNodeName The name of the query node in the executed Spark Plan. Often used to describe the command being executed.
     * @param jobType Identifies whether the process is a job (on a Dataproc cluster), a batch or a session.
     * @param sessionId Populated only for Dataproc serverless interactive sessions. The resource id of the session, used for URL generation.
     * @param sessionUuid Populated only for Dataproc serverless interactive sessions. A UUID generated by the service when it creates the session.
     */
    @JsonCreator
    private GcpDataprocRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("appId") String appId, @JsonProperty("appName") String appName,
        @JsonProperty("batchId") String batchId, @JsonProperty("batchUuid") String batchUuid,
        @JsonProperty("clusterName") String clusterName,
        @JsonProperty("clusterUuid") String clusterUuid, @JsonProperty("jobId") String jobId,
        @JsonProperty("jobUuid") String jobUuid, @JsonProperty("projectId") String projectId,
        @JsonProperty("queryNodeName") String queryNodeName,
        @JsonProperty("jobType") String jobType, @JsonProperty("sessionId") String sessionId,
        @JsonProperty("sessionUuid") String sessionUuid) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/GcpDataprocRunFacet.json#/$defs/GcpDataprocRunFacet");
      this.appId = appId;
      this.appName = appName;
      this.batchId = batchId;
      this.batchUuid = batchUuid;
      this.clusterName = clusterName;
      this.clusterUuid = clusterUuid;
      this.jobId = jobId;
      this.jobUuid = jobUuid;
      this.projectId = projectId;
      this.queryNodeName = queryNodeName;
      this.jobType = jobType;
      this.sessionId = sessionId;
      this.sessionUuid = sessionUuid;
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
     * @return Application ID set by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
     */
    public String getAppId() {
      return appId;
    }

    /**
     * @return App name which may be provided by the user, or some default is used by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
     */
    public String getAppName() {
      return appName;
    }

    /**
     * @return Populated only for Dataproc serverless batches. The resource id of the batch.
     */
    public String getBatchId() {
      return batchId;
    }

    /**
     * @return Populated only for Dataproc serverless batches. A UUID generated by the service when it creates the batch.
     */
    public String getBatchUuid() {
      return batchUuid;
    }

    /**
     * @return Populated only for Dataproc GCE workloads. The cluster name is unique within a GCP project.
     */
    public String getClusterName() {
      return clusterName;
    }

    /**
     * @return Populated only for Dataproc GCE workloads. A UUID generated by the service at the time of cluster creation.
     */
    public String getClusterUuid() {
      return clusterUuid;
    }

    /**
     * @return Populated only for Dataproc GCE workloads. If not specified by the user, the job ID will be provided by the service.
     */
    public String getJobId() {
      return jobId;
    }

    /**
     * @return Populated only for Dataproc GCE workloads. A UUID that uniquely identifies a job within the project over time.
     */
    public String getJobUuid() {
      return jobUuid;
    }

    /**
     * @return The GCP project ID that the resource belongs to.
     */
    public String getProjectId() {
      return projectId;
    }

    /**
     * @return The name of the query node in the executed Spark Plan. Often used to describe the command being executed.
     */
    public String getQueryNodeName() {
      return queryNodeName;
    }

    /**
     * @return Identifies whether the process is a job (on a Dataproc cluster), a batch or a session.
     */
    public String getJobType() {
      return jobType;
    }

    /**
     * @return Populated only for Dataproc serverless interactive sessions. The resource id of the session, used for URL generation.
     */
    public String getSessionId() {
      return sessionId;
    }

    /**
     * @return Populated only for Dataproc serverless interactive sessions. A UUID generated by the service when it creates the session.
     */
    public String getSessionUuid() {
      return sessionUuid;
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
      GcpDataprocRunFacet that = (GcpDataprocRunFacet) o;
      if (!Objects.equals(appId, that.appId)) return false;
      if (!Objects.equals(appName, that.appName)) return false;
      if (!Objects.equals(batchId, that.batchId)) return false;
      if (!Objects.equals(batchUuid, that.batchUuid)) return false;
      if (!Objects.equals(clusterName, that.clusterName)) return false;
      if (!Objects.equals(clusterUuid, that.clusterUuid)) return false;
      if (!Objects.equals(jobId, that.jobId)) return false;
      if (!Objects.equals(jobUuid, that.jobUuid)) return false;
      if (!Objects.equals(projectId, that.projectId)) return false;
      if (!Objects.equals(queryNodeName, that.queryNodeName)) return false;
      if (!Objects.equals(jobType, that.jobType)) return false;
      if (!Objects.equals(sessionId, that.sessionId)) return false;
      if (!Objects.equals(sessionUuid, that.sessionUuid)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, appName, batchId, batchUuid, clusterName, clusterUuid, jobId, jobUuid, projectId, queryNodeName, jobType, sessionId, sessionUuid, additionalProperties);
    }
  }

  /**
   * builder class for GcpDataprocRunFacet
   */
  public final class GcpDataprocRunFacetBuilder implements Builder<GcpDataprocRunFacet> {
    private String appId;

    private String appName;

    private String batchId;

    private String batchUuid;

    private String clusterName;

    private String clusterUuid;

    private String jobId;

    private String jobUuid;

    private String projectId;

    private String queryNodeName;

    private String jobType;

    private String sessionId;

    private String sessionUuid;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param appId Application ID set by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
     * @return this
     */
    public GcpDataprocRunFacetBuilder appId(String appId) {
      this.appId = appId;
      return this;
    }

    /**
     * @param appName App name which may be provided by the user, or some default is used by the resource manager. For spark jobs, it is set in the spark configuration of the current context.
     * @return this
     */
    public GcpDataprocRunFacetBuilder appName(String appName) {
      this.appName = appName;
      return this;
    }

    /**
     * @param batchId Populated only for Dataproc serverless batches. The resource id of the batch.
     * @return this
     */
    public GcpDataprocRunFacetBuilder batchId(String batchId) {
      this.batchId = batchId;
      return this;
    }

    /**
     * @param batchUuid Populated only for Dataproc serverless batches. A UUID generated by the service when it creates the batch.
     * @return this
     */
    public GcpDataprocRunFacetBuilder batchUuid(String batchUuid) {
      this.batchUuid = batchUuid;
      return this;
    }

    /**
     * @param clusterName Populated only for Dataproc GCE workloads. The cluster name is unique within a GCP project.
     * @return this
     */
    public GcpDataprocRunFacetBuilder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    /**
     * @param clusterUuid Populated only for Dataproc GCE workloads. A UUID generated by the service at the time of cluster creation.
     * @return this
     */
    public GcpDataprocRunFacetBuilder clusterUuid(String clusterUuid) {
      this.clusterUuid = clusterUuid;
      return this;
    }

    /**
     * @param jobId Populated only for Dataproc GCE workloads. If not specified by the user, the job ID will be provided by the service.
     * @return this
     */
    public GcpDataprocRunFacetBuilder jobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    /**
     * @param jobUuid Populated only for Dataproc GCE workloads. A UUID that uniquely identifies a job within the project over time.
     * @return this
     */
    public GcpDataprocRunFacetBuilder jobUuid(String jobUuid) {
      this.jobUuid = jobUuid;
      return this;
    }

    /**
     * @param projectId The GCP project ID that the resource belongs to.
     * @return this
     */
    public GcpDataprocRunFacetBuilder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    /**
     * @param queryNodeName The name of the query node in the executed Spark Plan. Often used to describe the command being executed.
     * @return this
     */
    public GcpDataprocRunFacetBuilder queryNodeName(String queryNodeName) {
      this.queryNodeName = queryNodeName;
      return this;
    }

    /**
     * @param jobType Identifies whether the process is a job (on a Dataproc cluster), a batch or a session.
     * @return this
     */
    public GcpDataprocRunFacetBuilder jobType(String jobType) {
      this.jobType = jobType;
      return this;
    }

    /**
     * @param sessionId Populated only for Dataproc serverless interactive sessions. The resource id of the session, used for URL generation.
     * @return this
     */
    public GcpDataprocRunFacetBuilder sessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    /**
     * @param sessionUuid Populated only for Dataproc serverless interactive sessions. A UUID generated by the service when it creates the session.
     * @return this
     */
    public GcpDataprocRunFacetBuilder sessionUuid(String sessionUuid) {
      this.sessionUuid = sessionUuid;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public GcpDataprocRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of GcpDataprocRunFacet from the fields set in the builder
     */
    @Override
    public GcpDataprocRunFacet build() {
      GcpDataprocRunFacet __result = new GcpDataprocRunFacet(OpenLineage.this.producer, appId, appName, batchId, batchUuid, clusterName, clusterUuid, jobId, jobUuid, projectId, queryNodeName, jobType, sessionId, sessionUuid);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles
   */
  @JsonDeserialize(
      as = DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles.class
  )
  @JsonPropertyOrder
  public static final class DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles {
    @JsonAnySetter
    private final Map<String, Double> additionalProperties;

    @JsonCreator
    private DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, Double> getAdditionalProperties() {
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
      DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles that = (DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles
   */
  public static final class DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantilesBuilder implements Builder<DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles> {
    private final Map<String, Double> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantilesBuilder put(String key,
        Double value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles from the fields set in the builder
     */
    @Override
    public DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles build() {
      DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles __result = new DataQualityMetricsDatasetFacetColumnMetricsAdditionalQuantiles();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for IcebergCommitReportOutputDatasetFacet
   */
  @JsonDeserialize(
      as = IcebergCommitReportOutputDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "snapshotId",
      "sequenceNumber",
      "operation",
      "commitMetrics",
      "metadata"
  })
  public static final class IcebergCommitReportOutputDatasetFacet implements OutputDatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Double snapshotId;

    private final Double sequenceNumber;

    private final String operation;

    private final IcebergCommitReportOutputDatasetFacetCommitMetrics commitMetrics;

    private final IcebergCommitReportOutputDatasetFacetMetadata metadata;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param snapshotId Snapshot ID of the iceberg table
     * @param sequenceNumber Sequence number of the iceberg table
     * @param operation Operation that was performed on the iceberg table
     * @param commitMetrics the commitMetrics
     * @param metadata the metadata
     */
    @JsonCreator
    private IcebergCommitReportOutputDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("snapshotId") Double snapshotId,
        @JsonProperty("sequenceNumber") Double sequenceNumber,
        @JsonProperty("operation") String operation,
        @JsonProperty("commitMetrics") IcebergCommitReportOutputDatasetFacetCommitMetrics commitMetrics,
        @JsonProperty("metadata") IcebergCommitReportOutputDatasetFacetMetadata metadata) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-2/IcebergCommitReportOutputDatasetFacet.json#/$defs/IcebergCommitReportOutputDatasetFacet");
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
      this.operation = operation;
      this.commitMetrics = commitMetrics;
      this.metadata = metadata;
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
     * @return Snapshot ID of the iceberg table
     */
    public Double getSnapshotId() {
      return snapshotId;
    }

    /**
     * @return Sequence number of the iceberg table
     */
    public Double getSequenceNumber() {
      return sequenceNumber;
    }

    /**
     * @return Operation that was performed on the iceberg table
     */
    public String getOperation() {
      return operation;
    }

    public IcebergCommitReportOutputDatasetFacetCommitMetrics getCommitMetrics() {
      return commitMetrics;
    }

    public IcebergCommitReportOutputDatasetFacetMetadata getMetadata() {
      return metadata;
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
      IcebergCommitReportOutputDatasetFacet that = (IcebergCommitReportOutputDatasetFacet) o;
      if (!Objects.equals(snapshotId, that.snapshotId)) return false;
      if (!Objects.equals(sequenceNumber, that.sequenceNumber)) return false;
      if (!Objects.equals(operation, that.operation)) return false;
      if (!Objects.equals(commitMetrics, that.commitMetrics)) return false;
      if (!Objects.equals(metadata, that.metadata)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(snapshotId, sequenceNumber, operation, commitMetrics, metadata, additionalProperties);
    }
  }

  /**
   * builder class for IcebergCommitReportOutputDatasetFacet
   */
  public final class IcebergCommitReportOutputDatasetFacetBuilder implements Builder<IcebergCommitReportOutputDatasetFacet> {
    private Double snapshotId;

    private Double sequenceNumber;

    private String operation;

    private IcebergCommitReportOutputDatasetFacetCommitMetrics commitMetrics;

    private IcebergCommitReportOutputDatasetFacetMetadata metadata;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param snapshotId Snapshot ID of the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetBuilder snapshotId(Double snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    /**
     * @param sequenceNumber Sequence number of the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetBuilder sequenceNumber(Double sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
      return this;
    }

    /**
     * @param operation Operation that was performed on the iceberg table
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetBuilder operation(String operation) {
      this.operation = operation;
      return this;
    }

    /**
     * @param commitMetrics the commitMetrics
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetBuilder commitMetrics(
        IcebergCommitReportOutputDatasetFacetCommitMetrics commitMetrics) {
      this.commitMetrics = commitMetrics;
      return this;
    }

    /**
     * @param metadata the metadata
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetBuilder metadata(
        IcebergCommitReportOutputDatasetFacetMetadata metadata) {
      this.metadata = metadata;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public IcebergCommitReportOutputDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of IcebergCommitReportOutputDatasetFacet from the fields set in the builder
     */
    @Override
    public IcebergCommitReportOutputDatasetFacet build() {
      IcebergCommitReportOutputDatasetFacet __result = new IcebergCommitReportOutputDatasetFacet(OpenLineage.this.producer, snapshotId, sequenceNumber, operation, commitMetrics, metadata);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ColumnLineageDatasetFacetFields
   */
  @JsonDeserialize(
      as = ColumnLineageDatasetFacetFields.class
  )
  @JsonPropertyOrder
  public static final class ColumnLineageDatasetFacetFields {
    @JsonAnySetter
    private final Map<String, ColumnLineageDatasetFacetFieldsAdditional> additionalProperties;

    @JsonCreator
    private ColumnLineageDatasetFacetFields() {
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    public Map<String, ColumnLineageDatasetFacetFieldsAdditional> getAdditionalProperties() {
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
      ColumnLineageDatasetFacetFields that = (ColumnLineageDatasetFacetFields) o;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(additionalProperties);
    }
  }

  /**
   * builder class for ColumnLineageDatasetFacetFields
   */
  public static final class ColumnLineageDatasetFacetFieldsBuilder implements Builder<ColumnLineageDatasetFacetFields> {
    private final Map<String, ColumnLineageDatasetFacetFieldsAdditional> additionalProperties = new LinkedHashMap<>();

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ColumnLineageDatasetFacetFieldsBuilder put(String key,
        ColumnLineageDatasetFacetFieldsAdditional value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ColumnLineageDatasetFacetFields from the fields set in the builder
     */
    @Override
    public ColumnLineageDatasetFacetFields build() {
      ColumnLineageDatasetFacetFields __result = new ColumnLineageDatasetFacetFields();
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for HierarchyDatasetFacet
   */
  @JsonDeserialize(
      as = HierarchyDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "hierarchy"
  })
  public static final class HierarchyDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<HierarchyDatasetFacetLevel> hierarchy;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param hierarchy Dataset hierarchy levels (e.g. DATABASE -> SCHEMA -> TABLE), from highest to lowest level. The order is important
     */
    @JsonCreator
    private HierarchyDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("hierarchy") List<HierarchyDatasetFacetLevel> hierarchy) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/HierarchyDatasetFacet.json#/$defs/HierarchyDatasetFacet");
      this._deleted = null;
      this.hierarchy = hierarchy;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Dataset hierarchy levels (e.g. DATABASE -> SCHEMA -> TABLE), from highest to lowest level. The order is important
     */
    public List<HierarchyDatasetFacetLevel> getHierarchy() {
      return hierarchy;
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
      HierarchyDatasetFacet that = (HierarchyDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(hierarchy, that.hierarchy)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, hierarchy, additionalProperties);
    }
  }

  /**
   * builder class for HierarchyDatasetFacet
   */
  public final class HierarchyDatasetFacetBuilder implements Builder<HierarchyDatasetFacet> {
    private List<HierarchyDatasetFacetLevel> hierarchy;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param hierarchy Dataset hierarchy levels (e.g. DATABASE -> SCHEMA -> TABLE), from highest to lowest level. The order is important
     * @return this
     */
    public HierarchyDatasetFacetBuilder hierarchy(List<HierarchyDatasetFacetLevel> hierarchy) {
      this.hierarchy = hierarchy;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public HierarchyDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of HierarchyDatasetFacet from the fields set in the builder
     */
    @Override
    public HierarchyDatasetFacet build() {
      HierarchyDatasetFacet __result = new HierarchyDatasetFacet(OpenLineage.this.producer, hierarchy);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for ExecutionParametersRunFacet
   */
  @JsonDeserialize(
      as = ExecutionParametersRunFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "parameters"
  })
  public static final class ExecutionParametersRunFacet implements RunFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final List<ExecutionParameter> parameters;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param parameters The parameters passed to the Job at runtime
     */
    @JsonCreator
    private ExecutionParametersRunFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("parameters") List<ExecutionParameter> parameters) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-0/ExecutionParametersRunFacet.json#/$defs/ExecutionParametersRunFacet");
      this.parameters = parameters;
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
     * @return The parameters passed to the Job at runtime
     */
    public List<ExecutionParameter> getParameters() {
      return parameters;
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
      ExecutionParametersRunFacet that = (ExecutionParametersRunFacet) o;
      if (!Objects.equals(parameters, that.parameters)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(parameters, additionalProperties);
    }
  }

  /**
   * builder class for ExecutionParametersRunFacet
   */
  public final class ExecutionParametersRunFacetBuilder implements Builder<ExecutionParametersRunFacet> {
    private List<ExecutionParameter> parameters;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param parameters The parameters passed to the Job at runtime
     * @return this
     */
    public ExecutionParametersRunFacetBuilder parameters(List<ExecutionParameter> parameters) {
      this.parameters = parameters;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public ExecutionParametersRunFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of ExecutionParametersRunFacet from the fields set in the builder
     */
    @Override
    public ExecutionParametersRunFacet build() {
      ExecutionParametersRunFacet __result = new ExecutionParametersRunFacet(OpenLineage.this.producer, parameters);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for SymlinksDatasetFacet
   */
  @JsonDeserialize(
      as = SymlinksDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "identifiers"
  })
  public static final class SymlinksDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final List<SymlinksDatasetFacetIdentifiers> identifiers;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param identifiers the identifiers
     */
    @JsonCreator
    private SymlinksDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("identifiers") List<SymlinksDatasetFacetIdentifiers> identifiers) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet");
      this._deleted = null;
      this.identifiers = identifiers;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    public List<SymlinksDatasetFacetIdentifiers> getIdentifiers() {
      return identifiers;
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
      SymlinksDatasetFacet that = (SymlinksDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(identifiers, that.identifiers)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, identifiers, additionalProperties);
    }
  }

  /**
   * builder class for SymlinksDatasetFacet
   */
  public final class SymlinksDatasetFacetBuilder implements Builder<SymlinksDatasetFacet> {
    private List<SymlinksDatasetFacetIdentifiers> identifiers;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param identifiers the identifiers
     * @return this
     */
    public SymlinksDatasetFacetBuilder identifiers(
        List<SymlinksDatasetFacetIdentifiers> identifiers) {
      this.identifiers = identifiers;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public SymlinksDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of SymlinksDatasetFacet from the fields set in the builder
     */
    @Override
    public SymlinksDatasetFacet build() {
      SymlinksDatasetFacet __result = new SymlinksDatasetFacet(OpenLineage.this.producer, identifiers);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for BinarySubsetCondition
   */
  @JsonDeserialize(
      as = BinarySubsetCondition.class
  )
  @JsonPropertyOrder({
      "left",
      "right",
      "type",
      "operator"
  })
  public static final class BinarySubsetCondition {
    private final LocationSubsetCondition left;

    private final LocationSubsetCondition right;

    private final String type;

    private final String operator;

    /**
     * @param left the left
     * @param right the right
     * @param operator Allowed values: 'AND' or 'OR'
     */
    @JsonCreator
    private BinarySubsetCondition(@JsonProperty("left") LocationSubsetCondition left,
        @JsonProperty("right") LocationSubsetCondition right,
        @JsonProperty("operator") String operator) {
      this.left = left;
      this.right = right;
      this.type = "binary";
      this.operator = operator;
    }

    public LocationSubsetCondition getLeft() {
      return left;
    }

    public LocationSubsetCondition getRight() {
      return right;
    }

    public String getType() {
      return type;
    }

    /**
     * @return Allowed values: 'AND' or 'OR'
     */
    public String getOperator() {
      return operator;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BinarySubsetCondition that = (BinarySubsetCondition) o;
      if (!Objects.equals(left, that.left)) return false;
      if (!Objects.equals(right, that.right)) return false;
      if (!Objects.equals(operator, that.operator)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(left, right, operator);
    }
  }

  /**
   * builder class for BinarySubsetCondition
   */
  public static final class BinarySubsetConditionBuilder implements Builder<BinarySubsetCondition> {
    private LocationSubsetCondition left;

    private LocationSubsetCondition right;

    private String operator;

    /**
     * @param left the left
     * @return this
     */
    public BinarySubsetConditionBuilder left(LocationSubsetCondition left) {
      this.left = left;
      return this;
    }

    /**
     * @param right the right
     * @return this
     */
    public BinarySubsetConditionBuilder right(LocationSubsetCondition right) {
      this.right = right;
      return this;
    }

    /**
     * @param operator Allowed values: 'AND' or 'OR'
     * @return this
     */
    public BinarySubsetConditionBuilder operator(String operator) {
      this.operator = operator;
      return this;
    }

    /**
     * build an instance of BinarySubsetCondition from the fields set in the builder
     */
    @Override
    public BinarySubsetCondition build() {
      BinarySubsetCondition __result = new BinarySubsetCondition(left, right, operator);
      return __result;
    }
  }

  /**
   * model class for StorageDatasetFacet
   */
  @JsonDeserialize(
      as = StorageDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "storageLayer",
      "fileFormat"
  })
  public static final class StorageDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String storageLayer;

    private final String fileFormat;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param storageLayer Storage layer provider with allowed values: iceberg, delta.
     * @param fileFormat File format with allowed values: parquet, orc, avro, json, csv, text, xml.
     */
    @JsonCreator
    private StorageDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("storageLayer") String storageLayer,
        @JsonProperty("fileFormat") String fileFormat) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/StorageDatasetFacet.json#/$defs/StorageDatasetFacet");
      this._deleted = null;
      this.storageLayer = storageLayer;
      this.fileFormat = fileFormat;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return Storage layer provider with allowed values: iceberg, delta.
     */
    public String getStorageLayer() {
      return storageLayer;
    }

    /**
     * @return File format with allowed values: parquet, orc, avro, json, csv, text, xml.
     */
    public String getFileFormat() {
      return fileFormat;
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
      StorageDatasetFacet that = (StorageDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(storageLayer, that.storageLayer)) return false;
      if (!Objects.equals(fileFormat, that.fileFormat)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, storageLayer, fileFormat, additionalProperties);
    }
  }

  /**
   * builder class for StorageDatasetFacet
   */
  public final class StorageDatasetFacetBuilder implements Builder<StorageDatasetFacet> {
    private String storageLayer;

    private String fileFormat;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param storageLayer Storage layer provider with allowed values: iceberg, delta.
     * @return this
     */
    public StorageDatasetFacetBuilder storageLayer(String storageLayer) {
      this.storageLayer = storageLayer;
      return this;
    }

    /**
     * @param fileFormat File format with allowed values: parquet, orc, avro, json, csv, text, xml.
     * @return this
     */
    public StorageDatasetFacetBuilder fileFormat(String fileFormat) {
      this.fileFormat = fileFormat;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public StorageDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of StorageDatasetFacet from the fields set in the builder
     */
    @Override
    public StorageDatasetFacet build() {
      StorageDatasetFacet __result = new StorageDatasetFacet(OpenLineage.this.producer, storageLayer, fileFormat);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }

  /**
   * model class for InputDataset
   */
  @JsonDeserialize(
      as = InputDataset.class
  )
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

    /**
     * @param namespace The namespace containing that dataset
     * @param name The unique name for that dataset within that namespace
     * @param facets The facets for this dataset
     * @param inputFacets The input facets for this dataset.
     */
    @JsonCreator
    private InputDataset(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name, @JsonProperty("facets") DatasetFacets facets,
        @JsonProperty("inputFacets") InputDatasetInputFacets inputFacets) {
      this.namespace = namespace;
      this.name = name;
      this.facets = facets;
      this.inputFacets = inputFacets;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InputDataset that = (InputDataset) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(facets, that.facets)) return false;
      if (!Objects.equals(inputFacets, that.inputFacets)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name, facets, inputFacets);
    }
  }

  /**
   * builder class for InputDataset
   */
  public static final class InputDatasetBuilder implements Builder<InputDataset> {
    private String namespace;

    private String name;

    private DatasetFacets facets;

    private InputDatasetInputFacets inputFacets;

    /**
     * @param namespace The namespace containing that dataset
     * @return this
     */
    public InputDatasetBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The unique name for that dataset within that namespace
     * @return this
     */
    public InputDatasetBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param facets The facets for this dataset
     * @return this
     */
    public InputDatasetBuilder facets(DatasetFacets facets) {
      this.facets = facets;
      return this;
    }

    /**
     * @param inputFacets The input facets for this dataset.
     * @return this
     */
    public InputDatasetBuilder inputFacets(InputDatasetInputFacets inputFacets) {
      this.inputFacets = inputFacets;
      return this;
    }

    /**
     * build an instance of InputDataset from the fields set in the builder
     */
    @Override
    public InputDataset build() {
      InputDataset __result = new InputDataset(namespace, name, facets, inputFacets);
      return __result;
    }
  }

  /**
   * model class for JobIdentifier
   */
  @JsonDeserialize(
      as = JobIdentifier.class
  )
  @JsonPropertyOrder({
      "namespace",
      "name"
  })
  public static final class JobIdentifier {
    private final String namespace;

    private final String name;

    /**
     * @param namespace The namespace containing the job
     * @param name The unique name of a job within that namespace
     */
    @JsonCreator
    private JobIdentifier(@JsonProperty("namespace") String namespace,
        @JsonProperty("name") String name) {
      this.namespace = namespace;
      this.name = name;
    }

    /**
     * @return The namespace containing the job
     */
    public String getNamespace() {
      return namespace;
    }

    /**
     * @return The unique name of a job within that namespace
     */
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JobIdentifier that = (JobIdentifier) o;
      if (!Objects.equals(namespace, that.namespace)) return false;
      if (!Objects.equals(name, that.name)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name);
    }
  }

  /**
   * builder class for JobIdentifier
   */
  public static final class JobIdentifierBuilder implements Builder<JobIdentifier> {
    private String namespace;

    private String name;

    /**
     * @param namespace The namespace containing the job
     * @return this
     */
    public JobIdentifierBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * @param name The unique name of a job within that namespace
     * @return this
     */
    public JobIdentifierBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * build an instance of JobIdentifier from the fields set in the builder
     */
    @Override
    public JobIdentifier build() {
      JobIdentifier __result = new JobIdentifier(namespace, name);
      return __result;
    }
  }

  /**
   * model class for DatasourceDatasetFacet
   */
  @JsonDeserialize(
      as = DatasourceDatasetFacet.class
  )
  @JsonPropertyOrder({
      "_producer",
      "_schemaURL",
      "_deleted",
      "name",
      "uri"
  })
  public static final class DatasourceDatasetFacet implements DatasetFacet {
    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String name;

    private final URI uri;

    @JsonAnySetter
    private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha
     * @param name the name
     * @param uri the uri
     */
    @JsonCreator
    private DatasourceDatasetFacet(@JsonProperty("_producer") URI _producer,
        @JsonProperty("name") String name, @JsonProperty("uri") URI uri) {
      this._producer = _producer;
      this._schemaURL = URI.create("https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet");
      this._deleted = null;
      this.name = name;
      this.uri = uri;
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
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    public String getName() {
      return name;
    }

    public URI getUri() {
      return uri;
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
      DatasourceDatasetFacet that = (DatasourceDatasetFacet) o;
      if (!Objects.equals(_deleted, that._deleted)) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(uri, that.uri)) return false;
      if (!Objects.equals(additionalProperties, that.additionalProperties)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_deleted, name, uri, additionalProperties);
    }
  }

  /**
   * builder class for DatasourceDatasetFacet
   */
  public final class DatasourceDatasetFacetBuilder implements Builder<DatasourceDatasetFacet> {
    private String name;

    private URI uri;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    /**
     * @param name the name
     * @return this
     */
    public DatasourceDatasetFacetBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * @param uri the uri
     * @return this
     */
    public DatasourceDatasetFacetBuilder uri(URI uri) {
      this.uri = uri;
      return this;
    }

    /**
     * add additional properties
     * @param key the additional property name
     * @param value the additional property value
     * @return this
     */
    public DatasourceDatasetFacetBuilder put(String key, Object value) {
      this.additionalProperties.put(key, value);return this;
    }

    /**
     * build an instance of DatasourceDatasetFacet from the fields set in the builder
     */
    @Override
    public DatasourceDatasetFacet build() {
      DatasourceDatasetFacet __result = new DatasourceDatasetFacet(OpenLineage.this.producer, name, uri);
      __result.getAdditionalProperties().putAll(additionalProperties);
      return __result;
    }
  }
}
