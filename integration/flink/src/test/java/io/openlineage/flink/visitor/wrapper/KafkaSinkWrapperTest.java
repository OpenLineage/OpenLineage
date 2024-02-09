/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.testcontainers.shaded.org.apache.commons.lang3.reflect.FieldUtils;

class KafkaSinkWrapperTest {

  private Properties props = new Properties();
  private static Schema schema =
      SchemaBuilder.record("OutputEvent")
          .namespace("io.openlineage.flink.avro.event")
          .fields()
          .name("a")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  private KafkaSink kafkaSink;
  private KafkaSinkWrapper wrapper;
  private KafkaRecordSerializationSchema serializationSchema =
      mock(KafkaRecordSerializationSchema.class);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    props.put("bootstrap.servers", "server1;server2");
    kafkaSink =
        KafkaSink.builder()
            .setBootstrapServers("server1;server2")
            .setKafkaProducerConfig(props)
            .setRecordSerializer(serializationSchema)
            .build();

    wrapper = KafkaSinkWrapper.of(kafkaSink);
  }

  @Test
  void testGetProducerConfig() {
    assertEquals(
        props.get("bootstrap.servers"), wrapper.getKafkaProducerConfig().get("bootstrap.servers"));
  }

  @Test
  @SneakyThrows
  void testGetKafkaTopic() {
    Function<?, ?> topicSelector = mock(Function.class);
    Function noArgFunction = mock(Function.class);
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "topicSelector"))
          .thenReturn(Optional.ofNullable(topicSelector));
      when(WrapperUtils.getFieldValue(topicSelector.getClass(), topicSelector, "topicSelector"))
          .thenReturn(Optional.ofNullable(noArgFunction));
      when(noArgFunction.apply(null)).thenReturn("topic");

      assertEquals("topic", wrapper.getKafkaTopic());
    }
  }

  @Test
  @SneakyThrows
  void testGetDynamicKafkaTopic() {
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      when(WrapperUtils.getFieldValue(serializationSchema.getClass(), serializationSchema, "topic"))
          .thenReturn(Optional.ofNullable("topic"));
      assertEquals("topic", wrapper.getKafkaTopic());
    }
  }

  @Test
  void testGetAvroSchema() {
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      RegistryAvroSerializationSchema avroSerializationSchema =
          mock(RegistryAvroSerializationSchema.class);
      GenericDatumWriter genericDatumWriter = new GenericDatumWriter(schema);
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"))
          .thenReturn(Optional.of(avroSerializationSchema));

      when(WrapperUtils.invoke(
              AvroSerializationSchema.class, avroSerializationSchema, "getDatumWriter"))
          .thenReturn(Optional.of(genericDatumWriter));

      when(WrapperUtils.getFieldValue(GenericDatumWriter.class, genericDatumWriter, "root"))
          .thenReturn(Optional.of(schema));

      assertEquals(schema, wrapper.getAvroSchema().get());
    }
  }

  @Test
  void testGetAvroSchemaWhenNoValueSerializationSchemaPresent() {
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"))
          .thenReturn(Optional.empty());

      assertFalse(wrapper.getAvroSchema().isPresent());
    }
  }

  @Test
  void testGetAvroSchemaWhenNoDatumWriterPresent() {
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      RegistryAvroSerializationSchema avroSerializationSchema =
          mock(RegistryAvroSerializationSchema.class);
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"))
          .thenReturn(Optional.of(avroSerializationSchema));

      when(WrapperUtils.invoke(
              AvroSerializationSchema.class, avroSerializationSchema, "getDatumWriter"))
          .thenReturn(Optional.empty());

      assertFalse(wrapper.getAvroSchema().isPresent());
    }
  }

  @Test
  void testGetTopicsOfMultiTopicSink() throws IllegalAccessException {
    KafkaRecordSerializationSchema serializer =
        (KafkaRecordSerializationSchema)
            mock(
                KafkaTopicsDescriptor.class,
                withSettings().extraInterfaces(KafkaRecordSerializationSchema.class));

    when(((KafkaTopicsDescriptor) serializer).isFixedTopics()).thenReturn(true);
    when(((KafkaTopicsDescriptor) serializer).getFixedTopics())
        .thenReturn(Arrays.asList("topic1", "topic2"));

    FieldUtils.writeField(wrapper, "serializationSchema", serializer, true);

    assertThat(wrapper.getTopicsOfMultiTopicSink()).containsExactlyInAnyOrder("topic1", "topic2");
  }

  @Test
  void testGetSchemaOfMultiTopicSink() throws IllegalAccessException {
    KafkaRecordSerializationSchema serializer =
        new MultiTopicSerializationSchema(new String[] {"t1"});
    FieldUtils.writeField(wrapper, "serializationSchema", serializer, true);

    try (MockedStatic<AvroSerializationSchema> mockedStatic =
        mockStatic(AvroSerializationSchema.class)) {
      AvroSerializationSchema<SomeEvent> schema = mock(AvroSerializationSchema.class);
      when(AvroSerializationSchema.<SomeEvent>forSpecific(eq(SomeEvent.class))).thenReturn(schema);
      assertThat(wrapper.getSchemaOfMultiTopicSink().get()).isEqualTo(schema);
    }
  }
}
