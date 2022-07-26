/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.apache.avro.Schema.Type.NULL;

import io.openlineage.client.OpenLineage;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;

/** Utility class for translating Avro schema into open lineage schema */
public class AvroSchemaUtils {

  /**
   * Converts Avro {@link Schema} to {@link OpenLineage.SchemaDatasetFacet}
   *
   * @param openLineage Openlineage instance
   * @param avroSchema schema
   * @return schema dataset facet
   */
  public static OpenLineage.SchemaDatasetFacet convert(OpenLineage openLineage, Schema avroSchema) {
    OpenLineage.SchemaDatasetFacetBuilder builder = openLineage.newSchemaDatasetFacetBuilder();
    List<OpenLineage.SchemaDatasetFacetFields> fields = new LinkedList<>();

    avroSchema.getFields().stream()
        .forEach(
            avroField -> {
              fields.add(
                  openLineage.newSchemaDatasetFacetFields(
                      avroField.name(), getTypeName(avroField.schema()), avroField.doc()));
            });

    return builder.fields(fields).build();
  }

  private static String getTypeName(Schema fieldSchema) {
    // In case of union type containing some type and null type, non-null type name is returned.
    // For example, Avro type `"type": ["null", "long"]` is converted to `long`.
    return Optional.of(fieldSchema)
        .filter(Schema::isUnion)
        .map(Schema::getTypes)
        .filter(types -> types.size() == 2) // check if schema field contains two types
        .filter(
            types ->
                types.get(0).getType().equals(NULL)
                    || types.get(1).getType().equals(NULL)) // check if one of the two types is NULL
        .stream()
        .flatMap(Collection::stream)
        .filter(type -> !type.getType().equals(NULL)) // if so, choose the non-null type
        .map(type -> type.getType().getName())
        .findAny()
        .orElse(fieldSchema.getType().getName());
  }
}
