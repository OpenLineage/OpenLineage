/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public class IcebergSchemaUtils {

  public static OpenLineage.SchemaDatasetFacet convert(OpenLineage openLineage, Schema schema) {
    return openLineage
        .newSchemaDatasetFacetBuilder()
        .fields(transformFields(openLineage, schema.columns()))
        .build();
  }

  private static List<SchemaDatasetFacetFields> transformFields(
      OpenLineage openLineage, List<Types.NestedField> fields) {
    return fields.stream()
        .map(field -> transformField(openLineage, field))
        .collect(Collectors.toList());
  }

  private static SchemaDatasetFacetFields transformField(
      OpenLineage openLineage, Types.NestedField field) {
    OpenLineage.SchemaDatasetFacetFieldsBuilder builder =
        openLineage
            .newSchemaDatasetFacetFieldsBuilder()
            .name(field.name())
            .type(field.type().typeId().name())
            .description(field.doc());

    if (field.type().isStructType()) {
      return builder
          .type("STRUCT")
          .fields(transformFields(openLineage, field.type().asStructType().fields()))
          .build();
    }
    if (field.type().isMapType()) {
      Types.MapType mapType = field.type().asMapType();
      return builder
          .type("MAP")
          .fields(
              transformFields(
                  openLineage,
                  List.of(
                      Types.NestedField.of(mapType.keyId(), false, "key", mapType.keyType()),
                      Types.NestedField.of(
                          mapType.valueId(),
                          mapType.isValueOptional(),
                          "value",
                          mapType.valueType()))))
          .build();
    }
    if (field.type().isListType()) {
      Types.ListType listType = field.type().asListType();
      return builder
          .type("LIST")
          .fields(
              transformFields(
                  openLineage,
                  List.of(
                      Types.NestedField.of(
                          0, listType.isElementOptional(), "_element", listType.elementType()))))
          .build();
    }
    return builder.build();
  }
}
