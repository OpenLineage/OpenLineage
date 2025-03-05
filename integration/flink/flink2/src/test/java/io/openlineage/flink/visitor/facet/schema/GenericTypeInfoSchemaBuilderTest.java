/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GenericTypeInfoSchemaBuilderTest {

  GenericTypeInfo genericTypeInfo = new GenericTypeInfo(TestingTypeClass.class);
  GenericTypeInfoSchemaBuilder builder = new GenericTypeInfoSchemaBuilder();
  List<SchemaDatasetFacetFields> facetFields;

  @BeforeEach
  public void beforeEach() {
    facetFields = builder.buildSchemaFields(genericTypeInfo);
  }

  @Test
  void testIsDefined() {
    assertThat(builder.isDefinedAt(genericTypeInfo)).isTrue();
    assertThat(builder.isDefinedAt(mock(PojoTypeInfo.class))).isFalse();
  }

  @Test
  void testPrimitiveType() {
    assertThat(getField("fieldA"))
        .hasFieldOrPropertyWithValue("name", "fieldA")
        .hasFieldOrPropertyWithValue("type", "String");

    assertThat(getField("fieldC"))
        .hasFieldOrPropertyWithValue("name", "fieldC")
        .hasFieldOrPropertyWithValue("type", "Long");
  }

  @Test
  void testPrivateFields() {
    assertThat(facetFields.stream().filter(f -> f.getName().equals("fieldB"))).isEmpty();
  }

  private SchemaDatasetFacetFields getField(String fieldName) {
    return facetFields.stream().filter(f -> f.getName().equals(fieldName)).findFirst().get();
  }

  private Optional<SchemaDatasetFacetFields> getField(
      List<SchemaDatasetFacetFields> facetFields, String fieldName) {
    return facetFields.stream().filter(f -> f.getName().equals(fieldName)).findFirst();
  }
}
