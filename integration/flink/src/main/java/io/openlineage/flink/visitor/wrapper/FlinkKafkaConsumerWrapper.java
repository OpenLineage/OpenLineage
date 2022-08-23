/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

@Slf4j
public class FlinkKafkaConsumerWrapper {

  private static final String DESERIALIZATION_SCHEMA_WRAPPER_CLASS =
      "org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchemaWrapper";

  private final FlinkKafkaConsumer flinkKafkaConsumer;

  private FlinkKafkaConsumerWrapper(FlinkKafkaConsumer flinkKafkaConsumer) {
    this.flinkKafkaConsumer = flinkKafkaConsumer;
  }

  public static FlinkKafkaConsumerWrapper of(FlinkKafkaConsumer flinkKafkaConsumer) {
    return new FlinkKafkaConsumerWrapper(flinkKafkaConsumer);
  }

  public Properties getKafkaProperties() {
    return getField("properties");
  }

  public List<String> getTopics() throws IllegalAccessException {
    KafkaTopicsDescriptor descriptor = getField("topicsDescriptor");
    if (descriptor.isFixedTopics()) {
      return descriptor.getFixedTopics();
    }

    // TODO: this will call Kafka. It's not clear whether we always can use it.
    Properties kafkaProperties = getField("properties");

    KafkaPartitionDiscoverer partitionDiscoverer =
        new KafkaPartitionDiscoverer(descriptor, 0, 0, kafkaProperties);
    return WrapperUtils.<List<String>>invoke(
            KafkaPartitionDiscoverer.class, partitionDiscoverer, "getAllTopics")
        .get();
  }

  public KafkaDeserializationSchema getDeserializationSchema() throws IllegalAccessException {
    return getField("deserializer");
  }

  public Optional<Schema> getAvroSchema() {
    try {
      final Class deserializationSchemaWrapperClass =
          Class.forName(DESERIALIZATION_SCHEMA_WRAPPER_CLASS);
      return Optional.of(getDeserializationSchema())
          .filter(el -> el.getClass().isAssignableFrom(deserializationSchemaWrapperClass))
          .flatMap(
              el ->
                  WrapperUtils.<DeserializationSchema>getFieldValue(
                      deserializationSchemaWrapperClass, el, "deserializationSchema"))
          .filter(schema -> schema instanceof AvroDeserializationSchema)
          .map(schema -> (AvroDeserializationSchema) schema)
          .map(schema -> schema.getProducedType())
          .flatMap(typeInformation -> Optional.ofNullable(typeInformation.getTypeClass()))
          .flatMap(aClass -> WrapperUtils.<Schema>invokeStatic(aClass, "getClassSchema"));
    } catch (ClassNotFoundException | IllegalAccessException e) {
      log.error("Cannot extract Avro schema: ", e);
      return Optional.empty();
    }
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(FlinkKafkaConsumer.class, flinkKafkaConsumer, name).get();
  }
}
