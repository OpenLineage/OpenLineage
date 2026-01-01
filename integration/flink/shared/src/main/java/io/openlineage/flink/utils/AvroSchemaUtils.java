/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.apache.avro.Schema.Type.NULL;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/** Utility class for translating Avro schema into open lineage schema */
@Slf4j
public class AvroSchemaUtils {

  /**
   * Converts Avro {@link Schema} to {@link OpenLineage.SchemaDatasetFacet}
   *
   * @param openLineage Openlineage instance
   * @param schema Avro schema
   * @return schema dataset facet
   */
  public static OpenLineage.SchemaDatasetFacet convert(OpenLineage openLineage, Schema schema) {
    return openLineage
        .newSchemaDatasetFacetBuilder()
        .fields(transformFields(openLineage, schema.getFields()))
        .build();
  }

  private static List<SchemaDatasetFacetFields> transformFields(
      OpenLineage openLineage, List<Schema.Field> fields) {
    return fields.stream()
        .map(field -> transformField(openLineage, field))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private static SchemaDatasetFacetFields transformField(
      OpenLineage openLineage, Schema.Field field) {
    log.debug("Converting avro field {} to OpenLineage field", field);
    OpenLineage.SchemaDatasetFacetFieldsBuilder builder =
        openLineage
            .newSchemaDatasetFacetFieldsBuilder()
            .name(field.name())
            .description(field.doc());

    List<Schema> fieldSchemas = excludeNulls(field.schema());
    if (fieldSchemas.size() > 1) {
      return builder
          .type("union")
          .fields(
              fieldSchemas.stream()
                  .map(
                      schema ->
                          transformField(
                              openLineage,
                              new Schema.Field("_" + schema.getType().getName(), schema)))
                  .collect(Collectors.toList()))
          .build();
    }

    Schema fieldSchema = fieldSchemas.get(0);
    Schema.Type fieldType = fieldSchema.getType();
    if (fieldType.equals(Schema.Type.RECORD)) {
      return builder
          .type(fieldSchema.getNamespace() + "." + fieldSchema.getName())
          .fields(transformFields(openLineage, fieldSchema.getFields()))
          .build();
    }
    if (fieldType.equals(Schema.Type.MAP)) {
      return builder
          .type("map")
          .fields(
              transformFields(
                  openLineage,
                  List.of(
                      new Schema.Field("key", Schema.create(Schema.Type.STRING)),
                      new Schema.Field("value", fieldSchema.getValueType()))))
          .build();
    }
    if (fieldType.equals(Schema.Type.ARRAY)) {
      return builder
          .type("array")
          .fields(
              transformFields(
                  openLineage, List.of(new Schema.Field("_element", fieldSchema.getElementType()))))
          .build();
    }
    if (fieldType.equals(Schema.Type.FIXED) || fieldType.equals(Schema.Type.ENUM)) {
      return builder.type(fieldSchema.getNamespace() + "." + fieldSchema.getName()).build();
    }
    return builder.type(fieldType.getName()).build();
  }

  private static List<Schema> excludeNulls(Schema fieldSchema) {
    // In case of union type containing some type and null type, only non-null types are returned.
    // For example, Avro type `"type": ["null", "long", "string"]` is converted to ["long",
    // "string"].
    if (!fieldSchema.isUnion()) {
      return List.of(fieldSchema);
    }
    return fieldSchema.getTypes().stream()
        .filter(type -> type.getType() != NULL)
        .collect(Collectors.toList());
  }
}
