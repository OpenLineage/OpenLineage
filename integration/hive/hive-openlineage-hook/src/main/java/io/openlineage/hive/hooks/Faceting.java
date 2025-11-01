/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.hooks;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetBuilder;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditional;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsBuilder;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.InputFieldBuilder;
import io.openlineage.client.OpenLineage.InputFieldTransformations;
import io.openlineage.client.OpenLineage.InputFieldTransformationsBuilder;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunBuilder;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacet;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacetIdentifiers;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.EventEmitter;
import io.openlineage.hive.client.HiveOpenLineageConfigParser;
import io.openlineage.hive.client.Versions;
import io.openlineage.hive.facets.HivePropertiesFacet;
import io.openlineage.hive.facets.HivePropertiesFacetBuilder;
import io.openlineage.hive.facets.HiveQueryInfoFacet;
import io.openlineage.hive.facets.HiveSessionInfoFacet;
import io.openlineage.hive.parsing.ColumnLineageCollector;
import io.openlineage.hive.parsing.Parsing;
import io.openlineage.hive.parsing.QueryExpr;
import io.openlineage.hive.util.HiveUtils;
import io.openlineage.hive.util.NetworkUtils;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.ExportWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hive.common.util.HiveVersionInfo;

@Slf4j
public class Faceting {

  public static void sanitizeEntity(Configuration conf, Entity entity) {
    // In some cases (e.g. CTAS) some metadata (e.g. the table location and column schemas)
    // isn't readily available in the read/write entities provided by the hook. So we run an
    // explicit call to the Hive Metastore to retrieve the values.
    if (entity.getTable().getSd().getCols().isEmpty()) {
      entity.setT(HiveUtils.getTable(conf, entity.getTable()));
    }
  }

  public static List<InputDataset> getInputDatasets(OpenLineageContext olContext) throws Exception {
    if (olContext.getHookContext().getQueryPlan().getOperation() == HiveOperation.LOAD
        || olContext.getHookContext().getQueryPlan().getOperation() == HiveOperation.IMPORT) {
      return getIdentityInputDataset(olContext);
    } else {
      return getQueryInputs(olContext);
    }
  }

  private static List<InputDataset> getIdentityInputDataset(OpenLineageContext olContext)
      throws Exception {
    // In case of LOAD, IMPORT operations, there is an assumption
    // that the input and output have the same schema
    // because of that we can compensate the lack of schema information in the read entity
    // in case of file inputs with the schema from the write entity
    if (olContext.getReadEntities().isEmpty()) {
      return Collections.emptyList();
    }

    ReadEntity readEntity = olContext.getReadEntities().iterator().next();
    DatasetIdentifier datasetIdentifier =
        FilesystemDatasetUtils.fromLocation(readEntity.getLocation());
    DatasetFacetsBuilder datasetFacetsBuilder =
        olContext.getOpenLineage().newDatasetFacetsBuilder();
    if (!olContext.getWriteEntities().isEmpty()) {
      datasetFacetsBuilder.schema(
          getSchemaDatasetFacet(
              olContext.getOpenLineage(),
              olContext.getWriteEntities().iterator().next().getTable()));
    }

    return Collections.singletonList(
        olContext
            .getOpenLineage()
            .newInputDatasetBuilder()
            .namespace(datasetIdentifier.getNamespace())
            .name(datasetIdentifier.getName())
            .facets(datasetFacetsBuilder.build())
            .build());
  }

  private static List<InputDataset> getQueryInputs(OpenLineageContext olContext) {
    List<InputDataset> inputs = new ArrayList<>();
    for (ReadEntity input : olContext.getReadEntities()) {
      Entity.Type entityType = input.getType();
      if ((entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION)
          && !input.isDummy()) {
        sanitizeEntity(olContext.getHookContext().getConf(), input);
        Table table = input.getTable();
        DatasetIdentifier di = HiveUtils.getDatasetIdentifierFromTable(table);
        OpenLineage ol = olContext.getOpenLineage();
        SchemaDatasetFacet schemaFacet = getSchemaDatasetFacet(ol, table);
        SymlinksDatasetFacet symlinksDatasetFacet = getSymlinkFacets(ol, di);
        inputs.add(
            ol.newInputDatasetBuilder()
                .namespace(getDatasetNamespace(olContext))
                .name(di.getName())
                .facets(
                    ol.newDatasetFacetsBuilder()
                        .schema(schemaFacet)
                        .symlinks(symlinksDatasetFacet)
                        .build())
                .build());
      }
    }
    return inputs;
  }

  private static String getDatasetNamespace(OpenLineageContext olContext) {
    Configuration conf = olContext.getHookContext().getConf();
    if (conf.get("hive.metastore.uris") != null) {
      return conf.get("hive.metastore.uris").split(",")[0].replaceFirst(".*://", "hive://");
    }
    if (conf.get("hive.server2.thrift.bind.host") != null) {
      return String.format(
          "hive://%s:%s",
          conf.get("hive.server2.thrift.bind.host"), conf.get("hive.metastore.port"));
    }

    return String.format(
        "hive://%s:%s",
        NetworkUtils.LOCAL_IP_ADDRESS.getHostName(), conf.get("hive.metastore.port"));
  }

  public static List<OutputDataset> getOutputDatasets(
      OpenLineageContext olContext, List<InputDataset> inputDatasets) throws Exception {
    HookContext hookContext = olContext.getHookContext();
    if (hookContext.getQueryPlan().getOperation() == HiveOperation.EXPORT
        || hookContext.getQueryPlan().getOperation() == HiveOperation.LOAD
        || hookContext.getQueryPlan().getOperation() == HiveOperation.IMPORT) {
      return getIdentityOutputDataset(olContext, inputDatasets);
    }
    return getQueryOutputDatasets(olContext, inputDatasets, hookContext);
  }

  private static List<OutputDataset> getQueryOutputDatasets(
      OpenLineageContext olContext, List<InputDataset> inputDatasets, HookContext hookContext) {
    SemanticAnalyzer semanticAnalyzer =
        HiveUtils.analyzeQuery(
            hookContext.getConf(),
            hookContext.getQueryState(),
            hookContext.getQueryPlan().getQueryString());

    boolean datasetLineageEnabled =
        hookContext
            .getConf()
            .getBoolean(HiveOpenLineageConfigParser.DATASET_LINEAGE_ENABLED_KEY, true);

    List<OutputDataset> outputs = new ArrayList<>();
    for (WriteEntity output : olContext.getWriteEntities()) {
      Entity.Type entityType = output.getType();
      if ((entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION)
          && !output.isDummy()) {
        sanitizeEntity(olContext.getHookContext().getConf(), output);
        Table outputTable = output.getTable();
        DatasetIdentifier di = HiveUtils.getDatasetIdentifierFromTable(outputTable);
        OpenLineage ol = olContext.getOpenLineage();
        SchemaDatasetFacet schemaFacet = getSchemaDatasetFacet(ol, outputTable);
        SymlinksDatasetFacet symlinksFacet = getSymlinkFacets(ol, di);
        DatasetFacetsBuilder datasetFacetsBuilder =
            ol.newDatasetFacetsBuilder().schema(schemaFacet).symlinks(symlinksFacet);
        QueryExpr query =
            Parsing.buildQueryTree(semanticAnalyzer.getQB(), outputTable.getFullyQualifiedName());
        OutputCLL outputCLL = ColumnLineageCollector.collectCLL(query, outputTable);
        ColumnLineageDatasetFacet columnsFacet =
            getColumnFacets(ol, datasetLineageEnabled, inputDatasets, outputCLL);
        if (!columnsFacet.getFields().getAdditionalProperties().isEmpty()) {
          datasetFacetsBuilder.columnLineage(columnsFacet);
        }
        outputs.add(
            ol.newOutputDatasetBuilder()
                .namespace(getDatasetNamespace(olContext))
                .name(di.getName())
                .facets(datasetFacetsBuilder.build())
                .build());
      }
    }
    return outputs;
  }

  private static List<OutputDataset> getIdentityOutputDataset(
      OpenLineageContext olContext, List<InputDataset> inputDatasets) throws Exception {
    // In case of EXPORT operation there is always a single output and single input
    // both have the same schema and all the columns are have and IDENTITY transformation
    // so we can skip the column lineage analysis.
    // In case of specifying a partition, we put the partitioning field as a FILTER transformation
    if (olContext.getWriteEntities().isEmpty()) {
      return Collections.emptyList();
    }
    WriteEntity writeEntity = olContext.getWriteEntities().iterator().next();
    DatasetIdentifier datasetIdentifier;
    if (olContext.getHookContext().getQueryPlan().getOperation() == HiveOperation.EXPORT) {
      datasetIdentifier = FilesystemDatasetUtils.fromLocation(writeEntity.getLocation());
    } else {
      DatasetIdentifier di = HiveUtils.getDatasetIdentifierFromTable(writeEntity.getTable());
      datasetIdentifier =
          new DatasetIdentifier(di.getName(), getDatasetNamespace(olContext), di.getSymlinks());
    }

    InputDataset inputDataset = inputDatasets.get(0);
    return Collections.singletonList(
        olContext
            .getOpenLineage()
            .newOutputDatasetBuilder()
            .namespace(datasetIdentifier.getNamespace())
            .name(datasetIdentifier.getName())
            .facets(
                olContext
                    .getOpenLineage()
                    .newDatasetFacetsBuilder()
                    .schema(inputDataset.getFacets().getSchema())
                    .symlinks(getSymlinkFacets(olContext.getOpenLineage(), datasetIdentifier))
                    .columnLineage(getIdentityCLL(inputDataset, olContext))
                    .build())
            .build());
  }

  private static ColumnLineageDatasetFacet getIdentityCLL(
      InputDataset inputDataset, OpenLineageContext olContext) {
    List<SchemaDatasetFacetFields> fields = inputDataset.getFacets().getSchema().getFields();
    OpenLineage ol = olContext.getOpenLineage();
    ColumnLineageDatasetFacetFieldsBuilder cldffb = ol.newColumnLineageDatasetFacetFieldsBuilder();
    fields.stream()
        .map(
            f ->
                ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                    .inputFields(
                        Collections.singletonList(
                            getInputField(inputDataset, f, TransformationInfo.identity())))
                    .build())
        .forEach(f -> cldffb.put(f.getInputFields().get(0).getField(), f));

    ColumnLineageDatasetFacetBuilder columnLineageDatasetFacetBuilder =
        ol.newColumnLineageDatasetFacetBuilder().fields(cldffb.build());

    if (olContext.getHookContext().getQueryPlan().getOperation() == HiveOperation.EXPORT) {
      // in case of EXPORT there is a possibility if filtering by using PARTITION keyword
      // so we need to put the partitioning columns in the dataset dependencies with FILTER
      // transformation
      ExportWork work =
          (ExportWork) olContext.getHookContext().getQueryPlan().getRootTasks().get(0).getWork();
      Map<String, String> partSpec = work.getTableSpec().getPartSpec();
      if (partSpec != null && !partSpec.isEmpty()) {
        List<InputField> datasetDependencyInputs =
            fields.stream()
                .filter(f -> partSpec.containsKey(f.getName()))
                .map(
                    f ->
                        getInputField(
                            inputDataset,
                            f,
                            TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER)))
                .collect(Collectors.toList());
        columnLineageDatasetFacetBuilder.dataset(datasetDependencyInputs);
      }
    }
    return columnLineageDatasetFacetBuilder.build();
  }

  private static InputField getInputField(
      InputDataset inputDataset,
      SchemaDatasetFacetFields f,
      TransformationInfo transformationInfo) {
    return new InputFieldBuilder()
        .field(f.getName())
        .name(inputDataset.getName())
        .namespace(inputDataset.getNamespace())
        .transformations(
            Collections.singletonList(transformationInfo.toInputFieldsTransformations()))
        .build();
  }

  public static InputDataset getInputDataset(Table table, List<InputDataset> inputDatasets) {
    for (InputDataset inputDataset : inputDatasets) {
      if (inputDataset.getName().equals(table.getFullyQualifiedName())) {
        return inputDataset;
      }
    }
    throw new IllegalArgumentException("Input dataset not found");
  }

  public static ColumnLineageDatasetFacet getColumnFacets(
      OpenLineage ol,
      boolean datasetLineageEnabled,
      List<InputDataset> inputDatasets,
      OutputCLL outputCLL) {
    OpenLineage.ColumnLineageDatasetFacetFields cllFields =
        getColumnInputFields(ol, datasetLineageEnabled, inputDatasets, outputCLL);
    ColumnLineageDatasetFacetBuilder columnLineageFacetBuilder =
        ol.newColumnLineageDatasetFacetBuilder();
    columnLineageFacetBuilder.fields(cllFields);
    if (datasetLineageEnabled) {
      columnLineageFacetBuilder.dataset(
          getInputFields(inputDatasets, outputCLL, outputCLL.getDatasetDependencies()));
    } else {
      columnLineageFacetBuilder.dataset(Collections.emptyList());
    }
    return columnLineageFacetBuilder.build();
  }

  public static OpenLineage.ColumnLineageDatasetFacetFields getColumnInputFields(
      OpenLineage ol,
      boolean datasetLineageEnabled,
      List<InputDataset> inputDatasets,
      OutputCLL outputCLL) {
    ColumnLineageDatasetFacetFieldsBuilder inputFieldsFacetBuilder =
        ol.newColumnLineageDatasetFacetFieldsBuilder();
    List<InputField> datasetDependencies;
    if (datasetLineageEnabled) {
      datasetDependencies = Collections.emptyList();
    } else {
      datasetDependencies =
          getInputFields(inputDatasets, outputCLL, outputCLL.getDatasetDependencies());
    }
    for (String outputColumn : outputCLL.getColumns()) {
      List<InputField> olInputFields =
          getInputFields(
              inputDatasets, outputCLL, outputCLL.getColumnDependencies().get(outputColumn));
      olInputFields.addAll(datasetDependencies);
      ColumnLineageDatasetFacetFieldsAdditional value =
          ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
              .inputFields(olInputFields)
              .build();
      if (!value.getInputFields().isEmpty()) {
        inputFieldsFacetBuilder.put(outputColumn, value);
      }
    }
    return inputFieldsFacetBuilder.build();
  }

  public static List<InputField> getInputFields(
      List<InputDataset> inputDatasets,
      OutputCLL outputCLL,
      Map<String, Set<TransformationInfo>> inputs) {
    List<InputField> olInputFields = new ArrayList<>();
    for (Map.Entry<String, Set<TransformationInfo>> input : inputs.entrySet()) {
      int lastDotIndex = input.getKey().lastIndexOf('.');
      String inputTableFQN = input.getKey().substring(0, lastDotIndex);
      String inputColumn = input.getKey().substring(lastDotIndex + 1);
      Table inputTable = outputCLL.getInputTables().get(inputTableFQN);
      InputDataset inputDataset = getInputDataset(inputTable, inputDatasets);
      InputFieldBuilder olFieldBuilder =
          new InputFieldBuilder()
              .namespace(inputDataset.getNamespace())
              .name(inputDataset.getName())
              .field(inputColumn);
      List<InputFieldTransformations> olTransformations = new ArrayList<>();
      for (TransformationInfo transformation : input.getValue()) {
        InputFieldTransformationsBuilder olTransformationBuilder =
            getInputFieldTransformationsBuilder(transformation);
        olTransformations.add(olTransformationBuilder.build());
      }
      olFieldBuilder.transformations(olTransformations);
      olInputFields.add(olFieldBuilder.build());
    }
    return olInputFields;
  }

  public static InputFieldTransformationsBuilder getInputFieldTransformationsBuilder(
      TransformationInfo transformation) {
    return new InputFieldTransformationsBuilder()
        .type(transformation.getType().name())
        .description(transformation.getDescription())
        .subtype(transformation.getSubType().name())
        .masking(transformation.getMasking());
  }

  public static SymlinksDatasetFacet getSymlinkFacets(OpenLineage ol, DatasetIdentifier di) {
    if (!di.getSymlinks().isEmpty()) {
      List<SymlinksDatasetFacetIdentifiers> symlinks =
          di.getSymlinks().stream()
              .map(
                  symlink ->
                      ol.newSymlinksDatasetFacetIdentifiersBuilder()
                          .name(symlink.getName())
                          .namespace(symlink.getNamespace())
                          .type(symlink.getType().toString())
                          .build())
              .collect(Collectors.toList());
      return ol.newSymlinksDatasetFacet(symlinks);
    }
    return null;
  }

  public static SchemaDatasetFacet getSchemaDatasetFacet(OpenLineage ol, Table table) {
    List<FieldSchema> columns = table.getAllCols();
    SchemaDatasetFacet schemaFacet = null;
    if (columns != null && !columns.isEmpty()) {
      List<SchemaDatasetFacetFields> fields = new ArrayList<>();
      for (FieldSchema column : columns) {
        fields.add(
            ol.newSchemaDatasetFacetFieldsBuilder()
                .name(column.getName())
                .type(column.getType())
                .description(column.getComment())
                .build());
      }
      schemaFacet = ol.newSchemaDatasetFacet(fields);
    }
    return schemaFacet;
  }

  public static OpenLineage.ProcessingEngineRunFacet getProcessingEngineFacet(
      OpenLineageContext olContext) {
    return olContext
        .getOpenLineage()
        .newProcessingEngineRunFacetBuilder()
        .name("hive")
        .version(HiveVersionInfo.getVersion())
        .openlineageAdapterVersion(Versions.getVersion())
        .build();
  }

  public static HivePropertiesFacet getHivePropertiesFacet(OpenLineageContext olContext) {
    return new HivePropertiesFacetBuilder(olContext.getHookContext().getConf()).build();
  }

  public static HiveQueryInfoFacet getHiveQueryInfoFacet(OpenLineageContext olContext) {
    HookContext hookContext = olContext.getHookContext();
    return new HiveQueryInfoFacet()
        .setQueryId(hookContext.getQueryState().getQueryId())
        .setOperationName(hookContext.getOperationName());
  }

  public static HiveSessionInfoFacet getHiveSessionInfoFacet(OpenLineageContext olContext) {
    HookContext hookContext = olContext.getHookContext();
    HiveSessionInfoFacet result =
        new HiveSessionInfoFacet()
            .setUsername(
                Optional.ofNullable(hookContext.getUserName()) // for HiveServer2
                    .filter(userName -> !userName.isEmpty() && !userName.equals("anonymous"))
                    .orElse(hookContext.getUgi().getShortUserName())) // for HiveCli
            .setClientIp(hookContext.getIpAddress())
            .setSessionId(hookContext.getSessionId());

    // populated by hive.server2.session.hook, as HookContext dones't have such information
    long creationTimestamp = hookContext.getConf().getLong("hive.session.creationTimestamp", 0);
    if (creationTimestamp > 0) {
      result.setCreationTime(
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(creationTimestamp), ZoneOffset.UTC));
    }

    return result;
  }

  public static RunEvent getRunEvent(EventEmitter emitter, OpenLineageContext olContext)
      throws Exception {
    OpenLineage ol = olContext.getOpenLineage();
    RunBuilder runBuilder =
        ol.newRunBuilder()
            .runId(emitter.getRunId())
            .facets(
                ol.newRunFacetsBuilder()
                    .processing_engine(getProcessingEngineFacet(olContext))
                    .parent(getParentRunFacet(olContext))
                    .put("hive_query", getHiveQueryInfoFacet(olContext))
                    .put("hive_session", getHiveSessionInfoFacet(olContext))
                    .put("hive_properties", getHivePropertiesFacet(olContext))
                    .build());

    List<InputDataset> inputDatasets = getInputDatasets(olContext);
    List<OutputDataset> outputDatasets = getOutputDatasets(olContext, inputDatasets);
    String jobName =
        generateJobName(emitter.getJobName(), outputDatasets, olContext.getHookContext());
    return ol.newRunEventBuilder()
        .eventType(olContext.getEventType())
        .eventTime(olContext.getEventTime())
        .run(runBuilder.build())
        .job(
            ol.newJobBuilder()
                .namespace(emitter.getJobNamespace())
                .name(jobName)
                .facets(
                    ol.newJobFacetsBuilder()
                        .sql(getSQLJobFacet(olContext))
                        .jobType(getJobTypeFacet(olContext))
                        .build())
                .build())
        .inputs(inputDatasets)
        .outputs(outputDatasets)
        .build();
  }

  public static OpenLineage.ParentRunFacet getParentRunFacet(OpenLineageContext olContext) {
    Optional<UUID> uuid = convertToUUID(olContext.getOpenLineageConfig().getParentRunId());
    if (!uuid.isPresent()
        || olContext.getOpenLineageConfig().getParentJobName() == null
        || olContext.getOpenLineageConfig().getParentJobNamespace() == null) {
      return null;
    }
    return olContext
        .getOpenLineage()
        .newParentRunFacetBuilder()
        .run(olContext.getOpenLineage().newParentRunFacetRun(uuid.get()))
        .job(
            olContext
                .getOpenLineage()
                .newParentRunFacetJob(
                    olContext.getOpenLineageConfig().getParentJobNamespace(),
                    olContext.getOpenLineageConfig().getParentJobName()))
        .build();
  }

  public static OpenLineage.SQLJobFacet getSQLJobFacet(OpenLineageContext olContext) {
    return olContext
        .getOpenLineage()
        .newSQLJobFacetBuilder()
        .query(olContext.getHookContext().getQueryPlan().getQueryString())
        .dialect("hive")
        .build();
  }

  public static OpenLineage.JobTypeJobFacet getJobTypeFacet(OpenLineageContext olContext) {
    return olContext
        .getOpenLineage()
        .newJobTypeJobFacetBuilder()
        .jobType("QUERY")
        .processingType("BATCH")
        .integration("HIVE")
        .build();
  }

  private static String generateJobName(
      String jobName, List<OutputDataset> outputDatasets, HookContext hookContext) {
    if (outputDatasets.isEmpty()) {
      return hackyJobName(jobName, hookContext);
    }
    return String.format("%s.%s", jobName.toLowerCase(), outputDatasets.get(0).getName());
  }

  private static Optional<UUID> convertToUUID(String uuid) {
    try {
      return Optional.ofNullable(uuid).map(UUID::fromString);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  // In the start events for IMPORT operation, the write entity is a database, not a table.
  // To send a start event we need job name so we extract it directly from the query.
  // FIXME: this is a hacky solution, we should find a better way to handle this case.
  private static String hackyJobName(String jobName, HookContext hookContext) {
    if (hookContext.getQueryPlan().getOperation() == HiveOperation.IMPORT
        && !hookContext.getOutputs().isEmpty()) {
      WriteEntity writeEntity = hookContext.getOutputs().iterator().next();
      if (writeEntity.getType() == Entity.Type.DATABASE) {
        String trimmedQuery =
            hookContext.getQueryPlan().getQueryString().trim().replaceAll("\\s+", " ");
        java.util.regex.Pattern pattern =
            java.util.regex.Pattern.compile(
                "(?i)IMPORT\\s+(?:EXTERNAL\\s+)?TABLE\\s+"
                    + "(?:`?([^`.\\s]+)`?\\.)?`?([^`.\\s]+)`?\\s*(?=FROM|PARTITION)",
                java.util.regex.Pattern.CASE_INSENSITIVE);

        java.util.regex.Matcher matcher = pattern.matcher(trimmedQuery);
        if (matcher.find()) {
          String database =
              writeEntity.getDatabase() == null
                  ? matcher.group(1)
                  : writeEntity.getDatabase().getName();
          String entityName =
              database == null
                  ? matcher.group(2)
                  : String.format("%s.%s", database, matcher.group(2));
          return String.format("%s.%s", jobName.toLowerCase(), entityName);
        }
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Cannot generate job name from query: %s",
            hookContext.getQueryPlan().getQueryString()));
  }
}
