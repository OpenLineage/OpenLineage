/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.utils.ProtobufUtils;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
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
  private final OpenLineageContext context;

  KafkaSinkWrapper(KafkaSink kafkaSink, OpenLineageContext context) {
    this.kafkaSink = kafkaSink;
    this.context = context;
    this.serializationSchema =
        WrapperUtils.<KafkaRecordSerializationSchema>getFieldValue(
                KafkaSink.class, kafkaSink, "recordSerializer")
            .get();
    log.debug("SerializationSchema is null: {}", serializationSchema == null);
  }

  public static KafkaSinkWrapper of(KafkaSink kafkaSink, OpenLineageContext context) {
    return new KafkaSinkWrapper(kafkaSink, context);
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
    log.debug("Extracting Kafka topic from: {}", serializationSchema);
    Optional<Function<?, ?>> topicSelectorOpt =
        WrapperUtils.<Function<?, ?>>getFieldValue(
            serializationSchema.getClass(), serializationSchema, "topicSelector");
    if (topicSelectorOpt.isPresent()) {
      Function<?, ?> function =
          (Function<?, ?>)
              WrapperUtils.getFieldValue(
                      topicSelectorOpt.get().getClass(), topicSelectorOpt.get(), "topicSelector")
                  .get();

      return (String) function.apply(null);
    } else {
      // assume the other implementation as topic as a field inside, for example
      // DynamicKafkaRecordSerializationSchema.
      Optional<String> topicOptional =
          WrapperUtils.getFieldValue(serializationSchema.getClass(), serializationSchema, "topic");

      return topicOptional.isPresent() ? topicOptional.get() : "";
    }
  }

  public Optional<SchemaDatasetFacet> getSchemaFacet() {
    Optional<SerializationSchema> optionalSchema =
        WrapperUtils.getFieldValue(
            serializationSchema.getClass(), serializationSchema, "valueSerializationSchema");
    if (optionalSchema.isPresent()) {
      log.debug(
          "ValueSerializationSchema is present when extracting schema facet: {}",
          optionalSchema.get());
      if (optionalSchema.get() instanceof AvroSerializationSchema) {
        log.debug("Extracting AvroSchema from {}", optionalSchema.get());
        return AvroUtils.getAvroSchema(optionalSchema)
            .map(s -> AvroSchemaUtils.convert(context.getOpenLineage(), s));
      } else if (ProtobufUtils.isProtobufSerializationSchema(optionalSchema.get())) {
        log.debug("Extracting Protobuf schema from {}", optionalSchema.get());
        return ProtobufUtils.convert(context.getOpenLineage(), optionalSchema.get());
      } else {
        log.warn("Unsupported valueSerializationSchema {}", serializationSchema);
        return Optional.empty();
      }
    } else {
      log.debug("ValueSerializationSchema is not present when extracting schema facet");
      return AvroUtils.getAvroSchema(
              WrapperUtils.getFieldValue(
                  serializationSchema.getClass(), serializationSchema, "valueSerialization"))
          .map(s -> AvroSchemaUtils.convert(context.getOpenLineage(), s));
    }
  }
}
