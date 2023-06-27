/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

/**
 * Wrapper class to extract hidden fields and call hidden methods on {@link KafkaSource} object. It
 * encapsulates all the reflection methods used on {@link KafkaSource}.
 */
@Slf4j
public class KafkaSourceWrapper {

  private static final String DESERIALIZATION_SCHEMA_WRAPPER_CLASS =
      "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaValueOnlyDeserializationSchemaWrapper";
  private final KafkaSource kafkaSource;

  @Getter private final KafkaSubscriber kafkaSubscriber;

  private KafkaSourceWrapper(KafkaSource kafkaSource, KafkaSubscriber kafkaSubscriber) {
    this.kafkaSource = kafkaSource;
    this.kafkaSubscriber = kafkaSubscriber;
  }

  public static KafkaSourceWrapper of(KafkaSource kafkaSource) throws IllegalAccessException {
    Field subscriberField = FieldUtils.getField(KafkaSource.class, "subscriber", true);
    KafkaSubscriber kafkaSubscriber = (KafkaSubscriber) subscriberField.get(kafkaSource);

    return new KafkaSourceWrapper(kafkaSource, kafkaSubscriber);
  }

  public KafkaSubscriber getSubscriber() {
    return kafkaSubscriber;
  }

  public Properties getProps() throws IllegalAccessException {
    return WrapperUtils.<Properties>getFieldValue(KafkaSource.class, kafkaSource, "props").get();
  }

  public List<String> getTopics() throws IllegalAccessException {
    Optional<List<String>> topics =
        WrapperUtils.<List<String>>getFieldValue(
            kafkaSubscriber.getClass(), kafkaSubscriber, "topics");

    if (topics.isPresent()) {
      return topics.get();
    }

    Optional<Pattern> topicPattern =
        WrapperUtils.<Pattern>getFieldValue(
            kafkaSubscriber.getClass(), kafkaSubscriber, "topicPattern");

    // TODO: write some unit test to this
    if (topicPattern.isPresent()) {
      KafkaTopicsDescriptor descriptor = new KafkaTopicsDescriptor(null, topicPattern.get());

      KafkaPartitionDiscoverer partitionDiscoverer =
          new KafkaPartitionDiscoverer(descriptor, 0, 0, getProps());
      WrapperUtils.<List<String>>invoke(
          KafkaPartitionDiscoverer.class, partitionDiscoverer, "initializeConnections");
      return WrapperUtils.<List<String>>invoke(
              KafkaPartitionDiscoverer.class, partitionDiscoverer, "getAllTopics")
          .get();
    }
    return Collections.emptyList();
  }

  public KafkaRecordDeserializationSchema getDeserializationSchema() throws IllegalAccessException {
    return WrapperUtils.<KafkaRecordDeserializationSchema>getFieldValue(
            KafkaSource.class, kafkaSource, "deserializationSchema")
        .get();
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
          .flatMap(
              typeInformation -> {
                if (typeInformation
                    .getTypeClass()
                    .equals(org.apache.avro.generic.GenericRecord.class)) {
                  // GenericRecordAvroTypeInfo -> try to extract private schema field
                  return WrapperUtils.<Schema>getFieldValue(
                      typeInformation.getClass(), typeInformation, "schema");
                } else {
                  return Optional.ofNullable(typeInformation.getTypeClass())
                      .flatMap(
                          aClass -> WrapperUtils.<Schema>invokeStatic(aClass, "getClassSchema"));
                }
              });
    } catch (ClassNotFoundException | IllegalAccessException e) {
      log.error("Cannot extract Avro schema: ", e);
      return Optional.empty();
    }
  }
}
