/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.utils.ProtobufUtilsTest.ProtoDeserializationSchema;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProtobufFieldResolverTest {

  OpenLineage openLineage = new OpenLineage(mock(URI.class));
  SchemaDatasetFacet facet =
      ProtobufUtils.convert(openLineage, new ProtoDeserializationSchema()).get();
  SchemaDatasetFacetFields subEvent = getSubField(facet.getFields(), "subEvent").get();

  @Test
  void testConvertNestedDataType() {
    // verify subEvent
    assertThat(subEvent.getName()).isEqualTo("subEvent");
    assertThat(subEvent.getType())
        .isEqualTo("io.openlineage.flink.proto.event.SubProtobufTestEvent");
    assertThat(subEvent.getDescription()).isNull();

    assertThat(subEvent.getFields()).hasSize(5);
    assertThat(getSubField(subEvent.getFields(), "id"))
        .isPresent()
        .map(f -> f.getType())
        .hasValue("string");
    assertThat(getSubField(subEvent.getFields(), "subSubEvent"))
        .isPresent()
        .map(f -> f.getType())
        .hasValue("io.openlineage.flink.proto.event.SubSubProtobufTestEvent");

    // verify subSubEvent
    SchemaDatasetFacetFields subSubEvent = getSubField(subEvent.getFields(), "subSubEvent").get();

    assertThat(subSubEvent.getFields()).hasSize(2);
    assertThat(getSubField(subSubEvent.getFields(), "id"))
        .isPresent()
        .map(f -> f.getType())
        .hasValue("string");
    assertThat(getSubField(subSubEvent.getFields(), "version"))
        .isPresent()
        .map(f -> f.getType())
        .hasValue("int64");
  }

  @Test
  void testConvertVerifyMessageArrayDataType() {
    SchemaDatasetFacetFields field = getSubField(subEvent.getFields(), "arrMessage").get();
    assertThat(field.getFields()).hasSize(1);
    assertThat(field.getType()).isEqualTo("array");
    assertThat(field.getName()).isEqualTo("arrMessage");
    assertThat(field.getDescription()).isNull();

    assertThat(field.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue(
            "type", "io.openlineage.flink.proto.event.SubSubProtobufTestEvent");

    assertThat(field.getFields().get(0).getFields())
        .hasSize(2); // fields of SubSubProtobufTestEvent
  }

  @Test
  void testConvertVerifyPrimitiveArrayDataType() {
    SchemaDatasetFacetFields field = getSubField(subEvent.getFields(), "arrPrimitive").get();
    assertThat(field.getFields()).hasSize(1);
    assertThat(field.getType()).isEqualTo("array");
    assertThat(field.getName()).isEqualTo("arrPrimitive");
    assertThat(field.getDescription()).isNull();

    assertThat(field.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue("type", "int64");

    assertThat(field.getFields().get(0).getFields()).isNull(); // no nested fields
  }

  @Test
  void testConvertVerifyMapDataType() {
    SchemaDatasetFacetFields field = getSubField(subEvent.getFields(), "someMap").get();
    assertThat(field.getFields()).hasSize(2);
    assertThat(field.getType()).isEqualTo("map");
    assertThat(field.getName()).isEqualTo("someMap");
    assertThat(field.getDescription()).isNull();

    assertThat(field.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "key")
        .hasFieldOrPropertyWithValue("type", "string");

    assertThat(field.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "value")
        .hasFieldOrPropertyWithValue("type", "int32");
  }

  @Test
  void testAny() {
    SchemaDatasetFacetFields anyField = getSubField(facet.getFields(), "testAnyOf").get();
    assertThat(anyField.getFields()).hasSize(2);
    assertThat(anyField.getType()).isEqualTo("google.protobuf.Any");
    assertThat(anyField.getName()).isEqualTo("testAnyOf");
    assertThat(anyField.getDescription()).isNull();

    assertThat(anyField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "type_url")
        .hasFieldOrPropertyWithValue("type", "string");

    assertThat(anyField.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "value")
        .hasFieldOrPropertyWithValue("type", "bytes");
  }

  /**
   * One of field was introduced in protobuf to save memory. However, in OpenLineage schema it shall
   * be represented by two separate fields.
   */
  @Test
  void testOneOf() {
    SchemaDatasetFacetFields oneOf1 = getSubField(facet.getFields(), "oneOf1").get();
    SchemaDatasetFacetFields oneOf2 = getSubField(facet.getFields(), "oneOf2").get();

    assertThat(oneOf1)
        .hasFieldOrPropertyWithValue("name", "oneOf1")
        .hasFieldOrPropertyWithValue("type", "string");
    assertThat(oneOf2)
        .hasFieldOrPropertyWithValue("name", "oneOf2")
        .hasFieldOrPropertyWithValue(
            "type", "io.openlineage.flink.proto.event.SubProtobufTestEvent");
  }

  private Optional<SchemaDatasetFacetFields> getSubField(
      List<SchemaDatasetFacetFields> fields, String subfield) {
    return fields.stream().filter(f -> subfield.equals(f.getName())).findAny();
  }
}
