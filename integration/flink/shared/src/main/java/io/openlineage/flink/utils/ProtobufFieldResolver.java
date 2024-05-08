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
    SchemaDatasetFacetFields facetField;
    if (field.getType().getJavaType().equals(JavaType.MESSAGE)) {
      // nested field encountered, array and map land here as well
      if (field.isMapField()) {
        facetField = resolveMapField(field);
      } else if (field.isRepeated()) {
        facetField = resolveArrayField(field);
      } else {
        facetField = resolveStructField(field);
      }
    } else {
      facetField = resolvePrimitiveTypeField(field);
    }

    return facetField;
  }

  private SchemaDatasetFacetFields resolveMapField(FieldDescriptor field) {
    return openLineage.newSchemaDatasetFacetFields(
        field.getName(), "map", "", resolve(field.getMessageType()));
  }

  private SchemaDatasetFacetFields resolveArrayField(FieldDescriptor field) {
    return openLineage.newSchemaDatasetFacetFields(
        field.getName(),
        "array",
        "",
        Collections.singletonList(
            openLineage.newSchemaDatasetFacetFields(
                "_element", getFieldType(field), "", resolve(field.getMessageType()))));
  }

  private SchemaDatasetFacetFields resolveStructField(FieldDescriptor field) {
    return openLineage.newSchemaDatasetFacetFields(
        field.getName(), getFieldType(field), "", resolve(field.getMessageType()));
  }

  private SchemaDatasetFacetFields resolvePrimitiveTypeField(FieldDescriptor field) {
    return openLineage.newSchemaDatasetFacetFields(
        field.getName(), getFieldType(field), "", Collections.emptyList());
  }

  private String getFieldType(FieldDescriptor field) {
    if (field.getJavaType().equals(JavaType.MESSAGE)) {
      return field.getMessageType().getFullName();
    } else {
      return field.getType().name().toLowerCase(Locale.ROOT);
    }
  }
}
