/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet.schema;

import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFieldsBuilder;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

/** Extracts schema facet from PojoType type information. AvroTypeInfo extends PojoTypeInfo. */
public class PojoTypeInfoSchemaBuilder implements TypeInformationSchemaBuilder {

  @Override
  public boolean isDefinedAt(TypeInformation typeInformation) {
    return typeInformation instanceof PojoTypeInfo;
  }

  // TODO: make sure this doesn't contain {"name":"SCHEMA$", "type":"class"} field

  @Override
  public List<SchemaDatasetFacetFields> buildSchemaFields(TypeInformation typeInformation) {
    PojoTypeInfo pojoTypeInfo = (PojoTypeInfo) typeInformation;
    return Arrays.stream(pojoTypeInfo.getTypeClass().getFields())
        .filter(f -> Modifier.isPublic(f.getModifiers()))
        .map(
            f ->
                new SchemaDatasetFacetFieldsBuilder()
                    .type(
                        f.getType()
                            .getSimpleName()) // TODO: go deeper to extract fields recursively
                    .name(f.getName())
                    .build())
        .collect(Collectors.toList());

    // TODO: refactor to separate classes
    // TODO: write more tests & support nested fields
  }
}
