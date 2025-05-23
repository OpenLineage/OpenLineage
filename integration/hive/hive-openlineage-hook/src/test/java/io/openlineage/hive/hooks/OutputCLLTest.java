/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
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
