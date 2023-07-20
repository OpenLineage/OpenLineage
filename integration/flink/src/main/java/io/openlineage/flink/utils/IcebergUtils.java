/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;

public class IcebergUtils {

  public static boolean hasClasses() {
    try {
      IcebergUtils.class
          .getClassLoader()
          .loadClass("org.apache.iceberg.flink.source.StreamingMonitorFunction");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static OpenLineage.SchemaDatasetFacet getSchema(OpenLineageContext context, Table table) {
    List<SchemaDatasetFacetFields> fields =
        table.schema().columns().stream()
            .map(
                field ->
                    context
                        .getOpenLineage()
                        .newSchemaDatasetFacetFields(
                            field.name(), field.type().typeId().name(), field.doc()))
            .collect(Collectors.toList());
    return context.getOpenLineage().newSchemaDatasetFacet(fields);
  }
}
