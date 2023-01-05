/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

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
    return AvroUtils.getRegistryAvroSchema(
        WrapperUtils.getFieldValue(
            serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"));
  }
}
