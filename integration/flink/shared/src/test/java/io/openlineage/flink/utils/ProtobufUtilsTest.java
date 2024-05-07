/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Message;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.flink.proto.event.ProtobufTestEvent;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;

class ProtobufUtilsTest {

  OpenLineage openLineage = new OpenLineage(mock(URI.class));

  @Test
  void testIsProtobufSerializationSchemaForProtoClass() {
    assertThat(ProtobufUtils.isProtobufSerializationSchema(new ProtoSerializationSchema()))
        .isTrue();
  }

  @Test
  void testIsProtobufSerializationSchemaForNonProtoClass() {
    assertThat(ProtobufUtils.isProtobufSerializationSchema(new NonProtoSerializationSchema()))
        .isFalse();
  }

  @Test
  void testConvertForOutputDataset() {
    Optional<SchemaDatasetFacet> facet =
        ProtobufUtils.convert(openLineage, new ProtoSerializationSchema());

    List<String[]> fields =
        facet.get().getFields().stream()
            .map(f -> new String[] {f.getName(), f.getType()})
            .collect(Collectors.toList());

    assertThat(fields)
        .hasSize(6)
        .contains(
            new String[] {"id", "string"},
            new String[] {"version", "int64"},
            new String[] {"subEvent", "io.openlineage.flink.proto.event.SubProtobufTestEvent"},
            new String[] {"testAnyOf", "google.protobuf.Any"},
            new String[] {"oneOf1", "string"},
            new String[] {"oneOf2", "io.openlineage.flink.proto.event.SubProtobufTestEvent"});
  }

  @Test
  void testConvertForInputDataset() {
    SchemaDatasetFacet facet =
        ProtobufUtils.convert(openLineage, new ProtoDeserializationSchema()).get();

    List<String[]> fields =
        facet.getFields().stream()
            .map(f -> new String[] {f.getName(), f.getType()})
            .collect(Collectors.toList());

    assertThat(fields)
        .hasSize(6)
        .containsExactlyInAnyOrder(
            new String[] {"id", "string"},
            new String[] {"version", "int64"},
            new String[] {"subEvent", "io.openlineage.flink.proto.event.SubProtobufTestEvent"},
            new String[] {"testAnyOf", "google.protobuf.Any"},
            new String[] {"oneOf1", "string"},
            new String[] {"oneOf2", "io.openlineage.flink.proto.event.SubProtobufTestEvent"});
  }

  @Test
  void testConvertSerializeWhenNoOuterClassFound() {
    assertThat(ProtobufUtils.convert(openLineage, new NonProtoMessageSerializationSchema()))
        .isEmpty();
  }

  @Test
  void testIsProtobufDeserializationSchemaForProtoClass() {
    assertThat(ProtobufUtils.isProtobufDeserializationSchema(new ProtoDeserializationSchema()))
        .isTrue();
  }

  @Test
  void testIsProtobufDeserializationSchemaForProtoClassWithProducedType() {
    TypeInformation typeInformation = mock(TypeInformation.class);
    when(typeInformation.getTypeClass()).thenReturn(ProtobufTestEvent.class);
    assertThat(
            ProtobufUtils.isProtobufDeserializationSchema(
                new ProtoDeserializationSchema() {
                  @Override
                  public TypeInformation<ProtobufTestEvent> getProducedType() {
                    return typeInformation;
                  }
                }))
        .isTrue();
  }

  @Test
  void testIsProtobufDeserializationSchemaForNonProtoClass() {
    assertThat(ProtobufUtils.isProtobufDeserializationSchema(new NonProtoDeserializationSchema()))
        .isFalse();
  }

  @Test
  void testConvertDeserializeWhenNoOuterClassFound() {
    assertThat(ProtobufUtils.convert(openLineage, new NonProtoMessageSerializationSchema()))
        .isEmpty();
  }

  static class ProtoSerializationSchema implements SerializationSchema<ProtobufTestEvent> {
    @Override
    public byte[] serialize(ProtobufTestEvent o) {
      return new byte[0];
    }
  }

  static class NonProtoSerializationSchema implements SerializationSchema {
    @Override
    public byte[] serialize(Object o) {
      return new byte[0];
    }
  }

  static class NonProtoMessageSerializationSchema implements SerializationSchema<Message> {
    @Override
    public byte[] serialize(Message o) {
      return new byte[0];
    }
  }

  static class ProtoDeserializationSchema implements DeserializationSchema<ProtobufTestEvent> {
    @Override
    public ProtobufTestEvent deserialize(byte[] bytes) throws IOException {
      return null;
    }

    @Override
    public boolean isEndOfStream(ProtobufTestEvent inputEvent) {
      return false;
    }

    @Override
    public TypeInformation<ProtobufTestEvent> getProducedType() {
      return null;
    }
  }

  static class NonProtoDeserializationSchema implements DeserializationSchema {

    @Override
    public Object deserialize(byte[] bytes) throws IOException {
      return null;
    }

    @Override
    public boolean isEndOfStream(Object o) {
      return false;
    }

    @Override
    public TypeInformation getProducedType() {
      return null;
    }
  }

  static class NonProtoMessageDeserializationSchema implements DeserializationSchema<Message> {
    @Override
    public Message deserialize(byte[] bytes) throws IOException {
      return null;
    }

    @Override
    public boolean isEndOfStream(Message message) {
      return false;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
      return null;
    }
  }
}
