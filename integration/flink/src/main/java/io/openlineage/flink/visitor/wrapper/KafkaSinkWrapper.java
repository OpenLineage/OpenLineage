/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

/**
 * Wrapper class to extract hidden fields and call hidden methods on {@link KafkaSink} object. It
 * encapsulates all the reflection methods used on {@link KafkaSink}.
 */
@Slf4j
public class KafkaSinkWrapper {

  private final KafkaSink kafkaSink;
  private final KafkaRecordSerializationSchema serializationSchema;

  private KafkaSinkWrapper(KafkaSink kafkaSink) {
    this.kafkaSink = kafkaSink;
    this.serializationSchema =
        WrapperUtils.<KafkaRecordSerializationSchema>getFieldValue(
                KafkaSink.class, kafkaSink, "recordSerializer")
            .get();
  }

  public static KafkaSinkWrapper of(KafkaSink kafkaSink) {
    return new KafkaSinkWrapper(kafkaSink);
  }

  public Properties getKafkaProducerConfig() {
    return WrapperUtils.<Properties>getFieldValue(KafkaSink.class, kafkaSink, "kafkaProducerConfig")
        .get();
  }

  public List<String> getTopicsOfMultiTopicSink() {
    return Optional.of(serializationSchema)
        .filter(serializationSchema -> serializationSchema instanceof KafkaTopicsDescriptor)
        .map(serializationSchema -> (KafkaTopicsDescriptor) serializationSchema)
        .filter(descriptor -> descriptor.isFixedTopics())
        .map(descriptor -> descriptor.getFixedTopics())
        .orElse(Collections.emptyList());
  }

  public Optional<SerializationSchema> getSchemaOfMultiTopicSink() {
    return Arrays.stream(serializationSchema.getClass().getGenericInterfaces())
        .filter(t -> TypeUtils.isAssignable(t, KafkaRecordSerializationSchema.class))
        .findFirst()
        .filter(t -> t instanceof ParameterizedType)
        .map(t -> (ParameterizedType) t)
        .map(t -> t.getActualTypeArguments())
        .filter(t -> t != null || t.length > 0)
        .map(t -> t[0])
        .filter(t -> t instanceof Class)
        .map(t -> AvroSerializationSchema.forSpecific((Class) t));
  }

  public String getKafkaTopic() throws IllegalAccessException {
    Function<?, ?> topicSelector =
        WrapperUtils.<Function<?, ?>>getFieldValue(
                serializationSchema.getClass(), serializationSchema, "topicSelector")
            .get();

    Function<?, ?> function =
        (Function<?, ?>)
            WrapperUtils.getFieldValue(topicSelector.getClass(), topicSelector, "topicSelector")
                .get();

    return (String) function.apply(null);
  }

  public Optional<Schema> getAvroSchema() {
    return AvroUtils.getAvroSchema(
        WrapperUtils.getFieldValue(
            serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"));
  }
}
