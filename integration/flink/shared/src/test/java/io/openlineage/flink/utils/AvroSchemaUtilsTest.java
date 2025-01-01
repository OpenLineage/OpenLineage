/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class AvroSchemaUtilsTest {
  OpenLineage openLineage = new OpenLineage(mock(URI.class));

  @Test
  void testConvertSchemaPrimitives() {
    Schema schema =
        SchemaBuilder.record("InputEvent")
            .namespace("io.openlineage.flink.avro.event")
            .fields()
            .name("optional_int_field")
            .doc("some-doc")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .name("long_field")
            .type()
            .longType()
            .noDefault()
            .name("float_field")
            .type()
            .floatType()
            .noDefault()
            .name("double_field")
            .type()
            .doubleType()
            .noDefault()
            .name("string_field")
            .type()
            .stringType()
            .noDefault()
            .name("bytes_field")
            .type()
            .bytesType()
            .noDefault()
            .name("boolean_field")
            .type()
            .booleanType()
            .noDefault()
            .name("fixed_field")
            .type(Schema.createFixed("MD5", null, "io.openlineage.flink.avro.event", 16))
            .noDefault()
            .name("enum_field")
            .type(
                Schema.createEnum(
                    "Status",
                    null,
                    "io.openlineage.flink.avro.event",
                    List.of("started", "finished")))
            .withDefault("finished")
            .endRecord();

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        AvroSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(9);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "optional_int_field")
        .hasFieldOrPropertyWithValue("type", "int")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(1))
        .hasFieldOrPropertyWithValue("name", "long_field")
        .hasFieldOrPropertyWithValue("type", "long")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(2))
        .hasFieldOrPropertyWithValue("name", "float_field")
        .hasFieldOrPropertyWithValue("type", "float")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(3))
        .hasFieldOrPropertyWithValue("name", "double_field")
        .hasFieldOrPropertyWithValue("type", "double")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(4))
        .hasFieldOrPropertyWithValue("name", "string_field")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(5))
        .hasFieldOrPropertyWithValue("name", "bytes_field")
        .hasFieldOrPropertyWithValue("type", "bytes")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(6))
        .hasFieldOrPropertyWithValue("name", "boolean_field")
        .hasFieldOrPropertyWithValue("type", "boolean")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(7))
        .hasFieldOrPropertyWithValue("name", "fixed_field")
        .hasFieldOrPropertyWithValue("type", "io.openlineage.flink.avro.event.MD5")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(fields.get(8))
        .hasFieldOrPropertyWithValue("name", "enum_field")
        .hasFieldOrPropertyWithValue("type", "io.openlineage.flink.avro.event.Status")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testConvertSchemaUnion() {
    Schema schema =
        SchemaBuilder.record("InputEvent")
            .namespace("io.openlineage.flink.avro.event")
            .fields()
            .name("union_field")
            .doc("some-doc")
            .type(
                Schema.createUnion(
                    Schema.create(Schema.Type.STRING),
                    Schema.create(Schema.Type.INT),
                    Schema.create(Schema.Type.FLOAT),
                    Schema.create(Schema.Type.NULL)))
            .withDefault("a")
            .endRecord();

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        AvroSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(1);

    OpenLineage.SchemaDatasetFacetFields unionField = fields.get(0);

    assertThat(unionField)
        .hasFieldOrPropertyWithValue("name", "union_field")
        .hasFieldOrPropertyWithValue("type", "union")
        .hasFieldOrPropertyWithValue("description", "some-doc");

    List<OpenLineage.SchemaDatasetFacetFields> nestedFields = unionField.getFields();

    assertThat(nestedFields.size()).isEqualTo(3);

    assertThat(nestedFields.get(0))
        .hasFieldOrPropertyWithValue("name", "_string")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(nestedFields.get(1))
        .hasFieldOrPropertyWithValue("name", "_int")
        .hasFieldOrPropertyWithValue("type", "int")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(nestedFields.get(2))
        .hasFieldOrPropertyWithValue("name", "_float")
        .hasFieldOrPropertyWithValue("type", "float")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testConvertSchemaStruct() {
    Schema subSubInputEvent =
        SchemaBuilder.record("SubSubInputEvent")
            .namespace("io.openlineage.flink.avro.event")
            .fields()
            .name("sub_sub_input_value")
            .type()
            .stringType()
            .noDefault()
            .endRecord();

    Schema subInputEvent =
        SchemaBuilder.record("SubInputEvent")
            .namespace("io.openlineage.flink.avro.event")
            .fields()
            .name("sub_input_value")
            .doc("another-doc")
            .type()
            .nullable()
            .longType()
            .noDefault()
            .name("sub_sub_input")
            .type(subSubInputEvent)
            .noDefault()
            .endRecord();

    Schema schema =
        SchemaBuilder.record("InputEvent")
            .namespace("io.openlineage.flink.avro.event")
            .fields()
            .name("value")
            .doc("some-doc")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .name("sub_input")
            .type(subInputEvent)
            .noDefault()
            .endRecord();

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        AvroSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(2);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "value")
        .hasFieldOrPropertyWithValue("type", "int")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields nestedField = fields.get(1);
    assertThat(nestedField)
        .hasFieldOrPropertyWithValue("name", "sub_input")
        .hasFieldOrPropertyWithValue("type", "io.openlineage.flink.avro.event.SubInputEvent")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> subFields = nestedField.getFields();

    assertThat(subFields.size()).isEqualTo(2);

    assertThat(subFields.get(0))
        .hasFieldOrPropertyWithValue("name", "sub_input_value")
        .hasFieldOrPropertyWithValue("type", "long")
        .hasFieldOrPropertyWithValue("description", "another-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields superNestedField = subFields.get(1);
    assertThat(superNestedField)
        .hasFieldOrPropertyWithValue("name", "sub_sub_input")
        .hasFieldOrPropertyWithValue("type", "io.openlineage.flink.avro.event.SubSubInputEvent")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> subSubFields = superNestedField.getFields();

    assertThat(subSubFields.size()).isEqualTo(1);

    assertThat(subSubFields.get(0))
        .hasFieldOrPropertyWithValue("name", "sub_sub_input_value")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testConvertSchemaMap() {
    Schema schema =
        SchemaBuilder.record("InputEvent")
            .namespace("io.openlineage.flink.avro.event")
            .fields()
            .name("optional_int_field")
            .doc("some-doc")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .name("map_field")
            .type(Schema.createMap(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .endRecord();

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        AvroSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(2);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "optional_int_field")
        .hasFieldOrPropertyWithValue("type", "int")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields mapField = fields.get(1);
    assertThat(mapField)
        .hasFieldOrPropertyWithValue("name", "map_field")
        .hasFieldOrPropertyWithValue("type", "map")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> mapSubFields = mapField.getFields();

    assertThat(mapSubFields.size()).isEqualTo(2);

    assertThat(mapSubFields.get(0))
        .hasFieldOrPropertyWithValue("name", "key")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(mapSubFields.get(1))
        .hasFieldOrPropertyWithValue("name", "value")
        .hasFieldOrPropertyWithValue("type", "long")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testConvertSchemaArray() {
    Schema schema =
        SchemaBuilder.record("InputEvent")
            .namespace("io.openlineage.flink.avro.event")
            .fields()
            .name("optional_int_field")
            .doc("some-doc")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .name("array_field")
            .type(Schema.createArray(Schema.create(Schema.Type.STRING)))
            .noDefault()
            .endRecord();

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        AvroSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertThat(fields.size()).isEqualTo(2);

    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "optional_int_field")
        .hasFieldOrPropertyWithValue("type", "int")
        .hasFieldOrPropertyWithValue("description", "some-doc")
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields listField = fields.get(1);
    assertThat(listField)
        .hasFieldOrPropertyWithValue("name", "array_field")
        .hasFieldOrPropertyWithValue("type", "array")
        .hasFieldOrPropertyWithValue("description", null);

    List<OpenLineage.SchemaDatasetFacetFields> listSubFields = listField.getFields();

    assertThat(listSubFields.size()).isEqualTo(1);

    assertThat(listSubFields.get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }
}
