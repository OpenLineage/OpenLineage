/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionDiscoverer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class KafkaSourceWrapperTest {

  private static final String DESERIALIZATION_SCHEMA = "deserializationSchema";
  private KafkaSubscriber kafkaSubscriber = mock(KafkaSubscriber.class);
  private Properties props = mock(Properties.class);
  private KafkaRecordDeserializationSchema deserializationSchema =
      mock(KafkaRecordDeserializationSchema.class);
  private OpenLineageContext context = mock(OpenLineageContext.class);
  private static Schema schema =
      SchemaBuilder.record("InputEvent")
          .namespace("io.openlineage.flink.avro.event")
          .fields()
          .name("a")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  private KafkaSource kafkaSource;
  private KafkaSourceWrapper wrapper;

  @BeforeEach
  @SneakyThrows
  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  public void setup() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(mock(URI.class)));
    Class kafkaSourceClass = Class.forName("org.apache.flink.connector.kafka.source.KafkaSource");
    Constructor<KafkaSource> constructor = kafkaSourceClass.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    kafkaSource =
        constructor.newInstance(kafkaSubscriber, null, null, null, deserializationSchema, props);
    wrapper = KafkaSourceWrapper.of(kafkaSource, context);
  }

  @Test
  @SneakyThrows
  void testGetSubscriber() {
    assertEquals(kafkaSubscriber, wrapper.getSubscriber());
  }

  @Test
  @SneakyThrows
  void testGetProps() {
    assertEquals(props, wrapper.getProps());
  }

  @Test
  @SneakyThrows
  void testGetDeserializationSchema() {
    assertEquals(deserializationSchema, wrapper.getDeserializationSchema());
  }

  @Test
  @SneakyThrows
  void testGetAvroSchema() {
    KafkaRecordDeserializationSchema deserializationSchema =
        (KafkaRecordDeserializationSchema)
            mock(
                Class.forName(
                    "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaValueOnlyDeserializationSchemaWrapper"));
    AvroDeserializationSchema avroDeserializationSchema = mock(AvroDeserializationSchema.class);
    TypeInformation typeInformation = mock(TypeInformation.class);

    when(avroDeserializationSchema.getProducedType()).thenReturn(typeInformation);
    when(typeInformation.getTypeClass())
        .thenReturn(this.getClass()); // test class contains getClassSchema method

    FieldUtils.writeField(kafkaSource, DESERIALIZATION_SCHEMA, deserializationSchema, true);
    FieldUtils.writeField(
        deserializationSchema, DESERIALIZATION_SCHEMA, avroDeserializationSchema, true);

    List<SchemaDatasetFacetFields> fields = wrapper.getSchemaFacet().get().getFields();
    assertThat(fields).hasSize(1);
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "long");
  }

  @Test
  @SneakyThrows
  void testGetDynamicAvroSchema() {
    KafkaRecordDeserializationSchema deserializationSchema =
        (KafkaRecordDeserializationSchema)
            mock(
                Class.forName(
                    "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaDeserializationSchemaWrapper"));

    KafkaDeserializationSchema dynamicDeserializationSchema =
        (KafkaDeserializationSchema)
            mock(
                Class.forName(
                    "org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaDeserializationSchema"));

    AvroRowDataDeserializationSchema avroDeserializationSchema =
        mock(AvroRowDataDeserializationSchema.class);
    TypeInformation typeInformation = mock(TypeInformation.class);
    when(avroDeserializationSchema.getProducedType()).thenReturn(typeInformation);
    when(typeInformation.getTypeClass())
        .thenReturn(this.getClass()); // test class contains getClassSchema method

    FieldUtils.writeField(kafkaSource, DESERIALIZATION_SCHEMA, deserializationSchema, true);
    FieldUtils.writeField(
        deserializationSchema, "kafkaDeserializationSchema", dynamicDeserializationSchema, true);
    FieldUtils.writeField(
        dynamicDeserializationSchema, "valueDeserialization", avroDeserializationSchema, true);

    List<SchemaDatasetFacetFields> fields = wrapper.getSchemaFacet().get().getFields();
    assertThat(fields).hasSize(1);
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "long");
  }

  @Test
  @SneakyThrows
  void testGetAvroSchemaForNonAvroDeserializationSchema() {
    KafkaRecordDeserializationSchema deserializationSchema =
        (KafkaRecordDeserializationSchema)
            mock(
                Class.forName(
                    "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaValueOnlyDeserializationSchemaWrapper"));

    FieldUtils.writeField(kafkaSource, DESERIALIZATION_SCHEMA, deserializationSchema, true);
    FieldUtils.writeField(
        deserializationSchema, DESERIALIZATION_SCHEMA, mock(DeserializationSchema.class), true);

    assertEquals(Optional.empty(), wrapper.getSchemaFacet());
  }

  @Test
  @SneakyThrows
  void testGetAvroSchemaForEmptyDeserializationSchema() {
    assertEquals(Optional.empty(), wrapper.getSchemaFacet());
  }

  @Test
  @SneakyThrows
  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  void testGetTopicsForTopicList() {
    List<String> topics = Arrays.asList("topic1", "topic2");
    Class kafkaSourceClass = Class.forName("org.apache.flink.connector.kafka.source.KafkaSource");
    Constructor<KafkaSource> constructor = kafkaSourceClass.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    KafkaSubscriber kafkaSubscriber = KafkaSubscriber.getTopicListSubscriber(topics);
    kafkaSource = constructor.newInstance(kafkaSubscriber, null, null, null, null, props);
    wrapper = KafkaSourceWrapper.of(kafkaSource, context);
    FieldUtils.writeField(kafkaSubscriber, "topics", topics, true);

    assertEquals(topics, wrapper.getTopics());
  }

  @Test
  @SneakyThrows
  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  void testGetTopicsForPattern() {
    Pattern pattern = Pattern.compile("topic.*");
    List<String> topics = Arrays.asList("topic1", "topic2");

    Class kafkaSourceClass = Class.forName("org.apache.flink.connector.kafka.source.KafkaSource");
    Constructor<KafkaSource> constructor = kafkaSourceClass.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    KafkaSubscriber kafkaSubscriber = KafkaSubscriber.getTopicPatternSubscriber(pattern);
    kafkaSource = constructor.newInstance(kafkaSubscriber, null, null, null, null, props);
    wrapper = KafkaSourceWrapper.of(kafkaSource, context);
    FieldUtils.writeField(kafkaSubscriber, "topicPattern", pattern, true);

    try (MockedStatic<WrapperUtils> wrapperUtils = mockStatic(WrapperUtils.class)) {
      when(WrapperUtils.<Properties>getFieldValue(KafkaSource.class, kafkaSource, "props"))
          .thenReturn(Optional.of(props));

      when(WrapperUtils.<Pattern>getFieldValue(
              kafkaSubscriber.getClass(), kafkaSubscriber, "topicPattern"))
          .thenReturn(Optional.of(pattern));

      when(WrapperUtils.invoke(eq(KafkaPartitionDiscoverer.class), any(), eq("getAllTopics")))
          .thenReturn(Optional.of(topics));

      assertEquals(topics, wrapper.getTopics());
    }
  }

  public static Schema getClassSchema() {
    return schema;
  }
}
