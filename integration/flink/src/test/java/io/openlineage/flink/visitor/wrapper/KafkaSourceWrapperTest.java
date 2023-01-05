/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang.reflect.FieldUtils;

class KafkaSourceWrapperTest {

  private static final String DESERIALIZATION_SCHEMA = "deserializationSchema";
  private KafkaSubscriber kafkaSubscriber = mock(KafkaSubscriber.class);
  private Properties props = mock(Properties.class);
  private KafkaRecordDeserializationSchema deserializationSchema =
      mock(KafkaRecordDeserializationSchema.class);
  private static Schema schema = mock(Schema.class);

  private KafkaSource kafkaSource;
  private KafkaSourceWrapper wrapper;

  @BeforeEach
  @SneakyThrows
  public void setup() {
    Class kafkaSourceClass = Class.forName("org.apache.flink.connector.kafka.source.KafkaSource");
    Constructor<KafkaSource> constructor = kafkaSourceClass.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    kafkaSource =
        constructor.newInstance(kafkaSubscriber, null, null, null, deserializationSchema, props);
    wrapper = KafkaSourceWrapper.of(kafkaSource);
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

    assertEquals(Optional.of(schema), wrapper.getAvroSchema());
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

    assertEquals(Optional.empty(), wrapper.getAvroSchema());
  }

  @Test
  @SneakyThrows
  void testGetAvroSchemaForEmptyDeserializationSchema() {
    assertEquals(Optional.empty(), wrapper.getAvroSchema());
  }

  public static Schema getClassSchema() {
    return schema;
  }
}
