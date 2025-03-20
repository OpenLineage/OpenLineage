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

import java.util.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;

@Slf4j
public class ColumnLineageExtractor {

  @Getter
  public static class InputFieldLineage {
    private final Table inputTable;
    private final String inputFieldName;

    public InputFieldLineage(Table inputTable, String inputFieldName) {
      this.inputTable = inputTable;
      this.inputFieldName = inputFieldName;
    }
  }

  @Getter
  public static class OutputFieldLineage {
    private final String outputFieldName;
    private final List<InputFieldLineage> inputFields;

    public OutputFieldLineage(String outputFieldName, List<InputFieldLineage> inputFields) {
      this.outputFieldName = outputFieldName;
      this.inputFields = inputFields;
    }
  }

  @Getter
  public static class ColumnLineage {
    Map<Table, List<OutputFieldLineage>> outputTables;

    ColumnLineage(Map<Table, List<OutputFieldLineage>> outputTables) {
      this.outputTables = outputTables;
    }
  }

  public static ColumnLineage getColumnLineage(QueryPlan plan, LineageCtx.Index index) {
    LinkedHashMap<String, ObjectPair<SelectOperator, Table>> finalSelOps =
        index.getFinalSelectOps();
    Map<Table, List<OutputFieldLineage>> outputTables = new HashMap<>();
    for (ObjectPair<SelectOperator, Table> pair : finalSelOps.values()) {
      SelectOperator finalSelOp = pair.getFirst();
      Table t = pair.getSecond();
      List<FieldSchema> fieldSchemas = getFieldSchemas(plan, t);
      List<String> columnNames = getColumnNames(t);
      Map<ColumnInfo, LineageInfo.Dependency> columnDependenciesMap =
          index.getDependencies(finalSelOp);
      List<LineageInfo.Dependency> dependencies = null;
      if (columnDependenciesMap != null) {
        dependencies = new ArrayList<>(columnDependenciesMap.values());
      }
      int fields = fieldSchemas.size();
      adjustFieldSchemasForDynamicKeys(t, columnDependenciesMap, fieldSchemas, columnNames);
      if (dependencies == null || dependencies.size() != fields) {
        log.info("Result schema has {} fields, but we don't get as many dependencies", fields);
      } else {
        Table outputTable = getOutputTable(plan, t);
        List<OutputFieldLineage> outputFieldLineages =
            createOutputFieldLineages(fieldSchemas, dependencies);
        outputTables.put(outputTable, outputFieldLineages);
      }
    }
    return new ColumnLineage(outputTables);
  }

  private static List<FieldSchema> getFieldSchemas(QueryPlan plan, Table t) {
    return t != null ? t.getCols() : plan.getResultSchema().getFieldSchemas();
  }

  private static Table getOutputTable(QueryPlan plan, Table t) {
    if (t != null) {
      return t;
    }
    for (WriteEntity output : plan.getOutputs()) {
      Entity.Type entityType = output.getType();
      if (entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION) {
        return output.getTable();
      }
    }
    return null;
  }

  private static List<String> getColumnNames(Table t) {
    return t != null && t.getCols() != null && !t.getCols().isEmpty()
        ? Utilities.getColumnNamesFromFieldSchema(t.getCols())
        : null;
  }

  private static void adjustFieldSchemasForDynamicKeys(
      Table t,
      Map<ColumnInfo, LineageInfo.Dependency> colMap,
      List<FieldSchema> fieldSchemas,
      List<String> colNames) {
    if (t != null && colMap != null && fieldSchemas.size() < colMap.size()) {
      List<FieldSchema> partitionKeys = t.getPartitionKeys();
      int dynamicKeyCount = colMap.size() - fieldSchemas.size();
      int keyOffset = partitionKeys.size() - dynamicKeyCount;
      if (keyOffset >= 0) {
        for (int i = 0; i < dynamicKeyCount; i++) {
          FieldSchema field = partitionKeys.get(keyOffset + i);
          fieldSchemas.add(field);
          if (colNames != null) {
            colNames.add(field.getName());
          }
        }
      }
    }
  }

  private static List<OutputFieldLineage> createOutputFieldLineages(
      List<FieldSchema> fieldSchemas, List<LineageInfo.Dependency> dependencies) {
    List<OutputFieldLineage> outputFieldLineages = new ArrayList<>();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      List<InputFieldLineage> inputFields = new ArrayList<>();
      LineageInfo.Dependency dependency = dependencies.get(i);
      for (LineageInfo.BaseColumnInfo baseColumnInfo : dependency.getBaseCols()) {
        inputFields.add(createInputFieldLineage(baseColumnInfo));
      }
      outputFieldLineages.add(
          new OutputFieldLineage(getOutputFieldName(i, fieldSchemas), inputFields));
    }
    return outputFieldLineages;
  }

  private static InputFieldLineage createInputFieldLineage(
      LineageInfo.BaseColumnInfo baseColumnInfo) {
    return new InputFieldLineage(
        new Table(baseColumnInfo.getTabAlias().getTable()), baseColumnInfo.getColumn().getName());
  }

  private static String getOutputFieldName(int fieldIndex, List<FieldSchema> fieldSchemas) {
    String fieldName = fieldSchemas.get(fieldIndex).getName();
    String[] parts = fieldName.split("\\.");
    return parts[parts.length - 1];
  }
}
