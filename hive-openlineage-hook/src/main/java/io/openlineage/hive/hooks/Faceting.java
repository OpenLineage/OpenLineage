/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.EventEmitter;
import io.openlineage.hive.client.HiveOpenLineageConfigParser;
import io.openlineage.hive.client.Versions;
import io.openlineage.hive.facets.HivePropertiesFacetBuilder;
import io.openlineage.hive.parsing.ColumnLineageCollector;
import io.openlineage.hive.parsing.Parsing;
import io.openlineage.hive.parsing.QueryExpr;
import io.openlineage.hive.util.HiveUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.common.util.HiveVersionInfo;

@Slf4j
public class Faceting {

  public static void sanitizeEntity(Configuration conf, Entity entity) {
    // In some cases (e.g. CTAS) some metadata (e.g. the table location and column schemas)
    // isn't readily available in the read/write entities provided by the hook. So we run an
    // explicit call to the Hive Metastore to retrieve the values.
    if (entity.getTable().getSd().getCols().isEmpty()) {
      entity.setT(
          HiveUtils.getTable(
              conf, entity.getTable().getDbName(), entity.getTable().getTableName()));
    }
  }

  public static List<InputDataset> getInputDatasets(OpenLineageContext olContext) {
    List<InputDataset> inputs = Collections.emptyList();
    for (ReadEntity input : olContext.getReadEntities()) {
      Entity.Type entityType = input.getType();
      if ((entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION)
          && !input.isDummy()) {
        sanitizeEntity(olContext.getHadoopConf(), input);
        Table table = input.getTable();
        DatasetIdentifier di = HiveUtils.getDatasetIdentifierFromTable(table);
        OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
        SchemaDatasetFacet schemaFacet = getSchemaDatasetFacet(olContext, table);
        SymlinksDatasetFacet symlinksDatasetFacet = getSymlinkFacets(ol, di);
        inputs.add(
            ol.newInputDatasetBuilder()
                .namespace(di.getNamespace())
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

  public static List<OutputDataset> getOutputDatasets(
      OpenLineageContext olContext, List<InputDataset> inputDatasets) {
    List<OutputDataset> outputs = new ArrayList<>();
    for (WriteEntity output : olContext.getWriteEntities()) {
      Entity.Type entityType = output.getType();
      if ((entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION)
          && !output.isDummy()) {
        sanitizeEntity(olContext.getHadoopConf(), output);
        Table outputTable = output.getTable();
        DatasetIdentifier di = HiveUtils.getDatasetIdentifierFromTable(outputTable);
        OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
        SchemaDatasetFacet schemaFacet = getSchemaDatasetFacet(olContext, outputTable);
        SymlinksDatasetFacet symlinksFacet = getSymlinkFacets(ol, di);
        DatasetFacetsBuilder datasetFacetsBuilder =
            ol.newDatasetFacetsBuilder().schema(schemaFacet).symlinks(symlinksFacet);
        QueryExpr query =
            Parsing.buildQueryTree(
                olContext.getSemanticAnalyzer().getQB(), outputTable.getFullyQualifiedName());
        OutputCLL outputCLL = ColumnLineageCollector.collectCLL(query, outputTable);
        if (olContext
                .getHadoopConf()
                .get(HiveOpenLineageConfigParser.CONF_PREFIX + "datasetLineageEnabled")
            != null) {
          // See:
          // https://github.com/OpenLineage/OpenLineage/blob/09bd1c83b4f0295a065c0220c5a12a0dab746f09/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ColumnLevelLineageUtils.java#L64-L67
          log.warn(
              "DEPRECATION WARNING: The `datasetLineageEnabled` will soon be removed (defaulting to true).");
        }
        boolean datasetLineageEnabled =
            olContext
                .getHadoopConf()
                .getBoolean(
                    HiveOpenLineageConfigParser.CONF_PREFIX + "datasetLineageEnabled", true);
        ColumnLineageDatasetFacet columnsFacet =
            getColumnFacets(ol, datasetLineageEnabled, inputDatasets, outputCLL);
        if (!columnsFacet.getFields().getAdditionalProperties().isEmpty()) {
          datasetFacetsBuilder.columnLineage(columnsFacet);
        }
        outputs.add(
            ol.newOutputDatasetBuilder()
                .namespace(di.getNamespace())
                .name(di.getName())
                .facets(datasetFacetsBuilder.build())
                .build());
      }
    }
    return outputs;
  }

  public static InputDataset getInputDataset(Table table, List<InputDataset> inputDatasets) {
    for (InputDataset inputDataset : inputDatasets) {
      if (inputDataset
          .getFacets()
          .getSymlinks()
          .getIdentifiers()
          .get(0)
          .getName()
          .equals(table.getFullyQualifiedName())) {
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
    List<InputField> olInputFields = Collections.emptyList();
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
      List<InputFieldTransformations> olTransformations = Collections.emptyList();
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
    InputFieldTransformationsBuilder olTransformationBuilder =
        new InputFieldTransformationsBuilder();
    olTransformationBuilder.type(transformation.getType().name());
    olTransformationBuilder.description(transformation.getDescription());
    olTransformationBuilder.subtype(transformation.getSubType().name());
    olTransformationBuilder.masking(transformation.getMasking());
    return olTransformationBuilder;
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

  public static SchemaDatasetFacet getSchemaDatasetFacet(
      OpenLineageContext olContext, Table table) {
    List<FieldSchema> columns = table.getCols();
    SchemaDatasetFacet schemaFacet = null;
    if (columns != null && !columns.isEmpty()) {
      List<SchemaDatasetFacetFields> fields = Collections.emptyList();
      for (FieldSchema column : columns) {
        fields.add(
            olContext
                .getOpenLineage()
                .newSchemaDatasetFacetFieldsBuilder()
                .name(column.getName())
                .type(column.getType())
                .description(column.getComment())
                .build());
      }
      schemaFacet = olContext.getOpenLineage().newSchemaDatasetFacet(fields);
    }
    return schemaFacet;
  }

  public static RunEvent getRunEvent(EventEmitter emitter, OpenLineageContext olContext) {
    OpenLineage ol = olContext.getOpenLineage();
    RunBuilder runBuilder =
        ol.newRunBuilder()
            .runId(emitter.getRunId())
            .facets(
                ol.newRunFacetsBuilder()
                    .put(
                        "processing_engine",
                        ol.newProcessingEngineRunFacetBuilder()
                            .name("hive")
                            .version(HiveVersionInfo.getVersion())
                            .openlineageAdapterVersion(
                                olContext.getOpenlineageHiveIntegrationVersion())
                            .build())
                    .put("hive_properties", new HivePropertiesFacetBuilder(olContext).build())
                    .build());
    List<InputDataset> inputDatasets = getInputDatasets(olContext);
    List<OutputDataset> outputDatasets = getOutputDatasets(olContext, inputDatasets);
    RunEvent runEvent =
        ol.newRunEventBuilder()
            .eventType(RunEvent.EventType.COMPLETE)
            .eventTime(olContext.getEventTime())
            .run(runBuilder.build())
            .job(
                ol.newJobBuilder()
                    .namespace(emitter.getJobNamespace())
                    .name(emitter.getJobName())
                    // TODO: Add job facets
                    .build())
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .build();
    return runEvent;
  }
}
