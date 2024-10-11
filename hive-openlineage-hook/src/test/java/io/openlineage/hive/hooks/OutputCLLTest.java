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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.junit.jupiter.api.Test;

class OutputCLLTest {

  private static OutputCLL createOutputCLL(List<FieldSchema> fields) {
    Table table = new Table("default", "test_table");
    table.setFields(fields);
    return new OutputCLL(table);
  }

  @Test
  void testInitialization() {
    List<FieldSchema> fields =
        Arrays.asList(
            new FieldSchema("col1", "string", null),
            new FieldSchema("col2", "int", null),
            new FieldSchema("col3", "boolean", null));
    OutputCLL outputCLL = createOutputCLL(fields);
    assertThat(outputCLL.getColumns()).isEqualTo(Arrays.asList("col1", "col2", "col3"));
    assertThat(outputCLL.getInputTables()).isEmpty();
    assertThat(outputCLL.getDatasetDependencies()).isEmpty();
  }
}
