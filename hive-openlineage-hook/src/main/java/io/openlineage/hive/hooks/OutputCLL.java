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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
