/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.kafka;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.flink.avro.event.OutputEvent;
import io.openlineage.flink.visitor.wrapper.FlinkKafkaProducerWrapper;
import java.util.regex.Pattern;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.openlineage.common.config.ConfigWrapper.fromResource;

public class KafkaClientProvider {

    private final static String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

    private KafkaClientProvider() {
    }

    public static KafkaSource<InputEvent> aKafkaSource(String... topics) {
        KafkaSourceBuilder<InputEvent> builder = KafkaSource.<InputEvent>builder()
            .setProperties(fromResource("kafka-consumer.conf").toProperties())
            .setBootstrapServers("kafka:9092")
            .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema
                .forSpecific(InputEvent.class, SCHEMA_REGISTRY_URL));

        if (topics.length == 1 && topics[0].contains(".*")) {
            // set topic pattern
            builder.setTopicPattern(Pattern.compile(topics[0]));
        } else {
            // set topics
            builder.setTopics(topics);
        }

        return builder.build();
    }


    public static FlinkKafkaConsumer<InputEvent> legacyKafkaSource(String... topics) {
        if (topics.length == 1 && topics[0].contains(".*")) {
            // set topic pattern
            return new FlinkKafkaConsumer(
                Pattern.compile(topics[0]),
                ConfluentRegistryAvroDeserializationSchema.forSpecific(InputEvent.class, SCHEMA_REGISTRY_URL),
                fromResource("kafka-consumer.conf").toProperties()
            );
        } else {
            // set topics
            return new FlinkKafkaConsumer(
                Arrays.asList(topics),
                ConfluentRegistryAvroDeserializationSchema.forSpecific(InputEvent.class, SCHEMA_REGISTRY_URL),
                fromResource("kafka-consumer.conf").toProperties()
            );
        }
    }

    public static KafkaSink<OutputEvent> aKafkaSink(String topic) {
        return KafkaSink.<OutputEvent>builder()
                .setKafkaProducerConfig(fromResource("kafka-producer.conf").toProperties())
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema
                                .forSpecific(OutputEvent.class, topic, SCHEMA_REGISTRY_URL))
                        .setTopic(topic)
                        .build())
                .build();
    }

    public static FlinkKafkaProducer<OutputEvent> legacyKafkaSink(String topic) {
        return new FlinkKafkaProducer<OutputEvent>(
          topic,
          ConfluentRegistryAvroSerializationSchema
            .forSpecific(OutputEvent.class, topic, SCHEMA_REGISTRY_URL),
          fromResource("kafka-producer.conf").toProperties()
        );
    }

}
