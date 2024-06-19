/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class PlanUtilsTest {
  @Test
  void testSchemaFacetFlat() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$);

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(2);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testSchemaFacetNested() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    StructType superNested = new StructType().add("d", IntegerType$.MODULE$);

    StructType nested =
        new StructType()
            .add("c", StringType$.MODULE$, false, "c description")
            .add("super_nested", superNested);

    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$)
            .add("nested", nested, true, "nested description");

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(3);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields nestedField = schemaDatasetFacet.getFields().get(2);
    assertThat(nestedField)
        .hasFieldOrPropertyWithValue("name", "nested")
        .hasFieldOrPropertyWithValue("type", "struct")
        .hasFieldOrPropertyWithValue("description", "nested description");

    assertThat(nestedField.getFields()).hasSize(2);

    assertThat(nestedField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "c")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "c description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(nestedField.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "super_nested")
        .hasFieldOrPropertyWithValue("type", "struct")
        .hasFieldOrPropertyWithValue("description", null);

    OpenLineage.SchemaDatasetFacetFields superNestedField = nestedField.getFields().get(1);
    assertThat(superNestedField.getFields()).hasSize(1);
    assertThat(superNestedField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "d")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testSchemaFacetMap() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    MapType map = new MapType(StringType$.MODULE$, IntegerType$.MODULE$, false);
    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$)
            .add("nested", map, true, "nested description");

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(3);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields mapField = schemaDatasetFacet.getFields().get(2);
    assertThat(mapField)
        .hasFieldOrPropertyWithValue("name", "nested")
        .hasFieldOrPropertyWithValue("type", "map")
        .hasFieldOrPropertyWithValue("description", "nested description");

    assertThat(mapField.getFields()).hasSize(2);

    assertThat(mapField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "key")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(mapField.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "value")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testSchemaFacetArray() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    StructType nested = new StructType().add("c", StringType$.MODULE$, false, "c description");
    ArrayType arrayNested = new ArrayType(nested, true);

    ArrayType arrayPrimitive = new ArrayType(StringType$.MODULE$, false);
    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$)
            .add("arrayPrimitive", arrayPrimitive, true, "arrayPrimitive description")
            .add("arrayNested", arrayNested, false);

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(4);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields arrayPrimitiveField =
        schemaDatasetFacet.getFields().get(2);
    assertThat(arrayPrimitiveField)
        .hasFieldOrPropertyWithValue("name", "arrayPrimitive")
        .hasFieldOrPropertyWithValue("type", "array")
        .hasFieldOrPropertyWithValue("description", "arrayPrimitive description");

    assertThat(arrayPrimitiveField.getFields()).hasSize(1);
    assertThat(arrayPrimitiveField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields arrayNestedField = schemaDatasetFacet.getFields().get(3);
    assertThat(arrayNestedField.getFields()).hasSize(1);
    assertThat(arrayNestedField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue("type", "struct")
        .hasFieldOrPropertyWithValue("description", null);

    OpenLineage.SchemaDatasetFacetFields arrayNestedElementField =
        arrayNestedField.getFields().get(0);
    assertThat(arrayNestedElementField.getFields()).hasSize(1);
    assertThat(arrayNestedElementField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "c")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "c description")
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testListOfAttributesToStructType() {
    Attribute attribute1 = mock(Attribute.class);
    when(attribute1.name()).thenReturn("a");
    when(attribute1.dataType()).thenReturn(StringType$.MODULE$);

    Attribute attribute2 = mock(Attribute.class);
    when(attribute2.name()).thenReturn("b");
    when(attribute2.dataType()).thenReturn(IntegerType$.MODULE$);

    StructType schema = PlanUtils.toStructType(Arrays.asList(attribute1, attribute2));

    assertThat(schema.fields()).hasSize(2);
    assertThat(schema.fields()[0])
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("dataType", StringType$.MODULE$)
        .hasFieldOrPropertyWithValue("nullable", false)
        .hasFieldOrPropertyWithValue("metadata", null);
    assertThat(schema.fields()[1])
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("dataType", IntegerType$.MODULE$)
        .hasFieldOrPropertyWithValue("nullable", false)
        .hasFieldOrPropertyWithValue("metadata", null);
  }
}
