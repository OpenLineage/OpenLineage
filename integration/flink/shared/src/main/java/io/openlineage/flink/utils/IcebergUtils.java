/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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

  public static OpenLineage.SchemaDatasetFacet getSchema(OpenLineageContext context, Table table) {
    List<SchemaDatasetFacetFields> fields =
        table.schema().columns().stream()
            .map(
                field ->
                    context
                        .getOpenLineage()
                        .newSchemaDatasetFacetFieldsBuilder()
                        .name(field.name())
                        .type(field.type().typeId().name())
                        .description(field.doc())
                        .build())
            .collect(Collectors.toList());
    return context.getOpenLineage().newSchemaDatasetFacet(fields);
  }
}
