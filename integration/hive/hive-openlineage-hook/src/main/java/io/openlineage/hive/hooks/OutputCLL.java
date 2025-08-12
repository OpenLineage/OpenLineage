/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.hooks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.openlineage.client.utils.TransformationInfo;
import lombok.Getter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

@Getter
public class OutputCLL {

  private final List<String> columns = new ArrayList<>();
  private final Map<String, Table> inputTables = new HashMap<>();
  private final Map<String, Map<String, Set<TransformationInfo>>> columnDependencies =
      new HashMap<>();
  private final Map<String, Set<TransformationInfo>> datasetDependencies = new HashMap<>();

  public OutputCLL(Table table) {
    for (FieldSchema fieldSchema : table.getCols()) {
      columns.add(fieldSchema.getName());
      columnDependencies.put(fieldSchema.getName(), new HashMap<>());
    }
  }
}
