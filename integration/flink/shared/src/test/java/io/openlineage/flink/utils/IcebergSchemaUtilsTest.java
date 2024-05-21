/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergSchemaUtilsTest {
  OpenLineage openLineage = new OpenLineage(mock(URI.class));

  @Test
  void testConvertSchemaFlat() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "a", Types.IntegerType.get(), "some-doc"),
            Types.NestedField.optional(2, "b", Types.StringType.get()));
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        IcebergSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(2);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "INTEGER")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "STRING")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testConvertSchemaStruct() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "a", Types.IntegerType.get(), "some-doc"),
            Types.NestedField.optional(
                2,
                "b",
                Types.StructType.of(
                    Types.NestedField.required(3, "c", Types.LongType.get(), "another-doc"),
                    Types.NestedField.required(
                        4,
                        "d",
                        Types.StructType.of(
                            Types.NestedField.required(5, "e", Types.StringType.get(), null))))));
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        IcebergSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(2);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "INTEGER")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields nestedField = fields.get(1);
    assertThat(nestedField)
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "STRUCT")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> subFields = nestedField.getFields();

    assertThat(subFields.size()).isEqualTo(2);

    assertThat(subFields.get(0))
        .hasFieldOrPropertyWithValue("name", "c")
        .hasFieldOrPropertyWithValue("type", "LONG")
        .hasFieldOrPropertyWithValue("description", "another-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields superNestedField = subFields.get(1);
    assertThat(superNestedField)
        .hasFieldOrPropertyWithValue("name", "d")
        .hasFieldOrPropertyWithValue("type", "STRUCT")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> subSubFields = superNestedField.getFields();

    assertThat(subSubFields.size()).isEqualTo(1);

    assertThat(subSubFields.get(0))
        .hasFieldOrPropertyWithValue("name", "e")
        .hasFieldOrPropertyWithValue("type", "STRING")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testConvertSchemaMap() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "a", Types.IntegerType.get(), "some-doc"),
            Types.NestedField.optional(
                2,
                "b",
                Types.MapType.ofRequired(3, 4, Types.LongType.get(), Types.StringType.get())));
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        IcebergSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(2);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "INTEGER")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields mapField = fields.get(1);
    assertThat(mapField)
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "MAP")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> mapSubFields = mapField.getFields();

    assertThat(mapSubFields.size()).isEqualTo(2);

    assertThat(mapSubFields.get(0))
        .hasFieldOrPropertyWithValue("name", "key")
        .hasFieldOrPropertyWithValue("type", "LONG")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(mapSubFields.get(1))
        .hasFieldOrPropertyWithValue("name", "value")
        .hasFieldOrPropertyWithValue("type", "STRING")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testConvertSchemaList() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "a", Types.IntegerType.get(), "some-doc"),
            Types.NestedField.optional(
                2, "b", Types.ListType.ofRequired(3, Types.StringType.get())));
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        IcebergSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(2);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "INTEGER")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields listField = fields.get(1);
    assertThat(listField)
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "LIST")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> listSubFields = listField.getFields();

    assertThat(listSubFields.size()).isEqualTo(1);

    assertThat(listSubFields.get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue("type", "STRING")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }
}
