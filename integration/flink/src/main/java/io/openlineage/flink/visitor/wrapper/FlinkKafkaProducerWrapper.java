/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

@Slf4j
public class FlinkKafkaProducerWrapper {
  FlinkKafkaProducer flinkKafkaProducer;

  private FlinkKafkaProducerWrapper(FlinkKafkaProducer flinkKafkaProducer) {
    this.flinkKafkaProducer = flinkKafkaProducer;
  }

  public static FlinkKafkaProducerWrapper of(FlinkKafkaProducer flinkKafkaProducer) {
    return new FlinkKafkaProducerWrapper(flinkKafkaProducer);
  }

  public String getKafkaTopic() {
    return getField("defaultTopicId");
  }

  public Properties getKafkaProducerConfig() {
    return getField("producerConfig");
  }

  public Optional<Schema> getAvroSchema() {
    Optional<KeyedSerializationSchema> keyedSchema =
        WrapperUtils.getFieldValue(FlinkKafkaProducer.class, flinkKafkaProducer, "keyedSchema");
    if (keyedSchema.isPresent()) {
      return getKeyedAvroSchema(keyedSchema.get());
    }
    return getKafkaAvroSchema();
  }

  private Optional<Schema> getKeyedAvroSchema(KeyedSerializationSchema serializationSchema) {
    if (serializationSchema instanceof KeyedSerializationSchemaWrapper) {
      return AvroUtils.getAvroSchema(
          WrapperUtils.getFieldValue(
              KeyedSerializationSchemaWrapper.class, serializationSchema, "serializationSchema"));
    }
    return Optional.empty();
  }

  private Optional<Schema> getKafkaAvroSchema() {
    KafkaSerializationSchema kafkaSchema = getField("kafkaSchema");
    if (kafkaSchema instanceof KafkaSerializationSchemaWrapper) {
      return AvroUtils.getAvroSchema(
          WrapperUtils.getFieldValue(
              KafkaSerializationSchemaWrapper.class, kafkaSchema, "serializationSchema"));
    }
    return Optional.empty();
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(FlinkKafkaProducer.class, flinkKafkaProducer, name).get();
  }
}
