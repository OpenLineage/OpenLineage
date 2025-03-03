/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.flink.StreamEnvironment.setupEnv;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.flink.avro.event.OutputEvent;
import java.util.regex.Pattern;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

public class FlinkTopicPatternApplication {

  private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = setupEnv(args);

    String bootstraps = parameters.getRequired("bootstraps");

    KafkaSource<InputEvent> source =
        KafkaSource.<InputEvent>builder()
            .setBootstrapServers(bootstraps)
            .setGroupId("testTopicPatternRead")
            .setTopicPattern(Pattern.compile(parameters.getRequired("input-topics")))
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                    InputEvent.class, SCHEMA_REGISTRY_URL))
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setBounded(OffsetsInitializer.latest())
            .build();

    KafkaSink<OutputEvent> sink =
        KafkaSink.<OutputEvent>builder()
            .setBootstrapServers(bootstraps)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setValueSerializationSchema(
                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                            OutputEvent.class, parameters.getRequired("output-topics"), SCHEMA_REGISTRY_URL))
                    .setTopic(parameters.getRequired("output-topics"))
                    .build())
            .build();

    DataStream<InputEvent> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");

    stream
        .keyBy(InputEvent::getId)
        .process(new StatefulCounter())
        .sinkTo(sink);
    env.execute();
  }
}
