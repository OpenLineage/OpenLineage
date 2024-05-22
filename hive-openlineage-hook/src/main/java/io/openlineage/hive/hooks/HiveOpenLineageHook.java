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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.EventEmitter;
import io.openlineage.hive.client.Versions;
import io.openlineage.hive.util.PathUtils;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveVersionInfo;

public class HiveOpenLineageHook implements ExecuteWithHookContext {

  private static final HashSet<String> OPERATION_NAMES = new HashSet<>();

  // TODO: Add all supported operations
  static {
    OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
    //    OPERATION_NAMES.add(HiveOperation.CREATETABLE.getOperationName());
    OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
    //    OPERATION_NAMES.add(HiveOperation.ALTERVIEW_AS.getOperationName());
    //    OPERATION_NAMES.add(HiveOperation.CREATEVIEW.getOperationName());
    //    OPERATION_NAMES.add(HiveOperation.ALTERTABLE_ADDCOLS.getOperationName());
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    QueryPlan queryPlan = hookContext.getQueryPlan();
    LineageCtx.Index index = hookContext.getIndex();
    SessionState sessionState = SessionState.get();
    if (hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK
        && sessionState != null
        && index != null
        && OPERATION_NAMES.contains(queryPlan.getOperationName())
        && !queryPlan.isExplain()) {
      emitOpenLineageEvent(hookContext, index);
    }
  }

  private List<OpenLineage.InputDataset> getInputDatasets(HookContext hookContext)
      throws Exception {
    List<OpenLineage.InputDataset> inputs = new ArrayList<>();
    for (ReadEntity input : hookContext.getQueryPlan().getInputs()) {
      Entity.Type entityType = input.getType();
      if (entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION) {
        Table table = input.getTable();
        DatasetIdentifier di = PathUtils.fromTable(table, hookContext.getConf());
        OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
        OpenLineage.SchemaDatasetFacet schemaFacet = getSchemaDatasetFacet(ol, table);
        OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet = getSymlinkFacets(ol, di);
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

  private List<OpenLineage.OutputDataset> getOutputDatasets(
      HookContext hookContext, LineageCtx.Index index) {
    ColumnLineageExtractor.ColumnLineage columnLineage =
        ColumnLineageExtractor.getColumnLineage(hookContext.getQueryPlan(), index);
    List<OpenLineage.OutputDataset> outputs = new ArrayList<>();
    for (WriteEntity output : hookContext.getQueryPlan().getOutputs()) {
      Entity.Type entityType = output.getType();
      if (entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION) {
        Table table = output.getTable();
        DatasetIdentifier di = PathUtils.fromTable(table, hookContext.getConf());
        OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
        OpenLineage.SchemaDatasetFacet schemaFacet = getSchemaDatasetFacet(ol, table);
        OpenLineage.SymlinksDatasetFacet symlinksFacet = getSymlinkFacets(ol, di);
        OpenLineage.ColumnLineageDatasetFacet columnsFacet =
            getColumnFacets(ol, columnLineage.getOutputTables().get(table));
        outputs.add(
            ol.newOutputDatasetBuilder()
                .namespace(di.getNamespace())
                .name(di.getName())
                .facets(
                    ol.newDatasetFacetsBuilder()
                        .schema(schemaFacet)
                        .symlinks(symlinksFacet)
                        .columnLineage(columnsFacet)
                        .build())
                .build());
      }
    }
    return outputs;
  }

  private OpenLineage.ColumnLineageDatasetFacet getColumnFacets(
      OpenLineage ol, List<ColumnLineageExtractor.OutputFieldLineage> outputLineages) {
    if (outputLineages == null) {
      return null;
    }
    OpenLineage.ColumnLineageDatasetFacetBuilder columnLineageFacetBuilder =
        ol.newColumnLineageDatasetFacetBuilder();
    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder inputFieldsFacetBuilder =
        ol.newColumnLineageDatasetFacetFieldsBuilder();
    for (ColumnLineageExtractor.OutputFieldLineage lineage : outputLineages) {
      List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> olInputFields =
          new ArrayList<>();
      for (ColumnLineageExtractor.InputFieldLineage fieldLineage : lineage.getInputFields()) {
        OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields olField =
            new OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder()
                // .namespace()  // TODO
                .name(fieldLineage.getInputTable().getFullyQualifiedName())
                .field(fieldLineage.getInputFieldName())
                .build();
        olInputFields.add(olField);
      }
      OpenLineage.ColumnLineageDatasetFacetFieldsAdditional value =
          ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
              .inputFields(olInputFields)
              .build();
      inputFieldsFacetBuilder.put(lineage.getOutputFieldName(), value);
    }
    columnLineageFacetBuilder.put("fields", inputFieldsFacetBuilder.build());
    return columnLineageFacetBuilder.build();
  }

  private OpenLineage.SymlinksDatasetFacet getSymlinkFacets(OpenLineage ol, DatasetIdentifier di) {
    if (!di.getSymlinks().isEmpty()) {
      List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlinks =
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

  private static OpenLineage.SchemaDatasetFacet getSchemaDatasetFacet(OpenLineage ol, Table table) {
    List<FieldSchema> columns = table.getCols();
    OpenLineage.SchemaDatasetFacet schemaFacet = null;
    if (columns != null && !columns.isEmpty()) {
      List<OpenLineage.SchemaDatasetFacetFields> fields = new ArrayList<>();
      for (FieldSchema column : columns) {
        fields.add(
            ol.newSchemaDatasetFacetFields(
                column.getName(), column.getType(), column.getComment()));
      }
      schemaFacet = ol.newSchemaDatasetFacet(fields);
    }
    return schemaFacet;
  }

  private void emitOpenLineageEvent(HookContext hookContext, LineageCtx.Index index)
      throws Exception {
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .build();
    OpenLineage ol = olContext.getOpenLineage();
    Configuration conf = hookContext.getConf();
    EventEmitter emitter = new EventEmitter(conf);
    OpenLineage.RunBuilder runBuilder =
        ol.newRunBuilder()
            .runId(emitter.getRunId())
            .facets(
                ol.newRunFacetsBuilder()
                    .put(
                        "processing_engine",
                        ol.newProcessingEngineRunFacetBuilder()
                            .name("hive")
                            .version(HiveVersionInfo.getVersion())
                            .put(
                                "execution_engine", hookContext.getQueryInfo().getExecutionEngine())
                            .openlineageAdapterVersion(
                                this.getClass().getPackage().getImplementationVersion())
                            .build())
                    .build());
    OpenLineage.RunEvent runEvent =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .eventTime(
                Instant.ofEpochMilli(hookContext.getQueryInfo().getBeginTime()) // FIXME?
                    .atZone(ZoneId.of("UTC")))
            .run(runBuilder.build())
            .job(
                ol.newJobBuilder()
                    .namespace(emitter.getJobNamespace())
                    .name(emitter.getJobName())
                    // TODO: Add job facets
                    //                    .facets(
                    //                        ol.newJobFacetsBuilder()
                    //                            .build())
                    .build())
            .inputs(getInputDatasets(hookContext))
            .outputs(getOutputDatasets(hookContext, index))
            .build();

    // Send the event
    ObjectMapper mapper = new ObjectMapper();
    Object jsonObject = mapper.readValue(OpenLineageClientUtils.toJson(runEvent), Object.class);
    String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
    System.out.println(prettyJson);
    emitter.emit(runEvent);
  }
}
