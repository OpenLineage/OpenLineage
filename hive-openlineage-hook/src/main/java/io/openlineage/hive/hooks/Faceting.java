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
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
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
import io.openlineage.hive.client.Versions;
import io.openlineage.hive.facets.HivePropertiesFacetBuilder;
import io.openlineage.hive.util.HiveUtils;
import io.openlineage.hive.util.PathUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.common.util.HiveVersionInfo;

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
    List<InputDataset> inputs = new ArrayList<>();
    for (ReadEntity input : olContext.getReadEntities()) {
      Entity.Type entityType = input.getType();
      if ((entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION)
          && !input.isDummy()) {
        sanitizeEntity(olContext.getHadoopConf(), input);
        Table table = input.getTable();
        DatasetIdentifier di = PathUtils.fromTable(table);
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
        DatasetIdentifier di = PathUtils.fromTable(outputTable);
        OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
        SchemaDatasetFacet schemaFacet = getSchemaDatasetFacet(olContext, outputTable);
        SymlinksDatasetFacet symlinksFacet = getSymlinkFacets(ol, di);
        DatasetFacetsBuilder datasetFacetsBuilder =
            ol.newDatasetFacetsBuilder().schema(schemaFacet).symlinks(symlinksFacet);
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
    throw new RuntimeException("Input dataset not found");
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
      List<SchemaDatasetFacetFields> fields = new ArrayList<>();
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
