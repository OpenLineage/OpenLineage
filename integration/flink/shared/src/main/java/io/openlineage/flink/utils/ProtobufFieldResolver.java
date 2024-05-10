/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.utils;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFieldsBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Class responsible for translating protbuf fields into {@link SchemaDatasetFacetFields} */
@Slf4j
public class ProtobufFieldResolver {

  private final OpenLineage openLineage;

  public ProtobufFieldResolver(OpenLineage openLineage) {
    this.openLineage = openLineage;
  }

  public List<SchemaDatasetFacetFields> resolve(Descriptor descriptor) {
    return descriptor.getFields().stream()
        .map(protoField -> resolveField(protoField))
        .collect(Collectors.toList());
  }

  /**
   * Resolves single Protobuf field into OpenLineage format.
   *
   * @param field
   * @return returns a list as a single oneOf field should result in two separate OpenLineage fields
   */
  public SchemaDatasetFacetFields resolveField(FieldDescriptor field) {
    log.debug("protoField: {} of type {}", field.getName(), field.getJavaType());
    if (field.isMapField()) {
      return resolveMapField(field);
    }
    if (field.isRepeated()) {
      return resolveArrayField(field);
    }
    if (field.getType().getJavaType().equals(JavaType.MESSAGE)) {
      return resolveStructField(field);
    }
    return resolvePrimitiveTypeField(field);
  }

  private SchemaDatasetFacetFields resolveMapField(FieldDescriptor field) {
    return openLineage
        .newSchemaDatasetFacetFieldsBuilder()
        .name(field.getName())
        .type("map")
        .fields(resolve(field.getMessageType()))
        .build();
  }

  private SchemaDatasetFacetFields resolveArrayField(FieldDescriptor field) {
    SchemaDatasetFacetFieldsBuilder elementBuilder =
        openLineage.newSchemaDatasetFacetFieldsBuilder().name("_element").type(getFieldType(field));

    // primitive types cannot have nested fields
    if (field.getJavaType().equals(JavaType.MESSAGE)) {
      elementBuilder.fields(resolve(field.getMessageType()));
    }
    return openLineage
        .newSchemaDatasetFacetFieldsBuilder()
        .name(field.getName())
        .type("array")
        .fields(Collections.singletonList(elementBuilder.build()))
        .build();
  }

  private SchemaDatasetFacetFields resolveStructField(FieldDescriptor field) {
    return openLineage
        .newSchemaDatasetFacetFieldsBuilder()
        .name(field.getName())
        .type(getFieldType(field))
        .fields(resolve(field.getMessageType()))
        .build();
  }

  private SchemaDatasetFacetFields resolvePrimitiveTypeField(FieldDescriptor field) {
    return openLineage
        .newSchemaDatasetFacetFieldsBuilder()
        .name(field.getName())
        .type(getFieldType(field))
        .build();
  }

  private String getFieldType(FieldDescriptor field) {
    if (field.getJavaType().equals(JavaType.MESSAGE)) {
      return field.getMessageType().getFullName();
    } else {
      return field.getType().name().toLowerCase(Locale.ROOT);
    }
  }
}
