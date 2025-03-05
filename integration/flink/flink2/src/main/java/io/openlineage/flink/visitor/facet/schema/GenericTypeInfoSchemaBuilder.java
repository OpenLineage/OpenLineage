/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet.schema;

import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFieldsBuilder;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

/** Interface for methods allowing translating Flink TypeInformation into OpenLineage facets. */
public class GenericTypeInfoSchemaBuilder implements TypeInformationSchemaBuilder {

  @Override
  public boolean isDefinedAt(TypeInformation typeInformation) {
    return typeInformation instanceof GenericTypeInfo;
  }

  @Override
  public List<SchemaDatasetFacetFields> buildSchemaFields(TypeInformation typeInformation) {
    GenericTypeInfo genericTypeInfo = (GenericTypeInfo) typeInformation;
    return Arrays.stream(genericTypeInfo.getTypeClass().getFields())
        .filter(f -> Modifier.isPublic(f.getModifiers()))
        .map(this::resolveField)
        .collect(Collectors.toList());
  }

  private SchemaDatasetFacetFields resolveField(Field field) {
    return new SchemaDatasetFacetFieldsBuilder()
        .type(field.getType().getSimpleName())
        .name(field.getName())
        .build();
  }
}
