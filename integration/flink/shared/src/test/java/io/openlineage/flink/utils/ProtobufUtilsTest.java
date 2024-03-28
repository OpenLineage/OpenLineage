/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Message;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.flink.proto.event.OutputEvent;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.SerializationSchema;
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

    assertThat(facet.get().getFields()).hasSize(3);

    List<String[]> fields =
        facet.get().getFields().stream()
            .map(f -> new String[] {f.getName(), f.getType()})
            .collect(Collectors.toList());

    assertThat(fields)
        .hasSize(3)
        .containsExactlyInAnyOrder(
            new String[] {"id", "string"},
            new String[] {"version", "long"},
            new String[] {"counter", "long"});
  }

  @Test
  void testConvertWhenNoOuterClassFound() {
    assertThat(ProtobufUtils.convert(openLineage, new NonProtoMessageSerializationSchema()))
        .isEmpty();
  }

  static class ProtoSerializationSchema implements SerializationSchema<OutputEvent> {
    @Override
    public byte[] serialize(OutputEvent o) {
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
}
