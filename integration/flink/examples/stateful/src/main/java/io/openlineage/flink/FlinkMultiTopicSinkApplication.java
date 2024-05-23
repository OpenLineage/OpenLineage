/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.common.config.ConfigWrapper.fromResource;
import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static io.openlineage.kafka.KafkaClientProvider.aKafkaSource;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.flink.avro.event.OutputEvent;
import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import java.util.Map;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FlinkMultiTopicSinkApplication {

  private static final String TOPIC_PARAM_SEPARATOR = ",";

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = setupEnv(args);
    String[] topics = parameters.getRequired("output-topics").split(TOPIC_PARAM_SEPARATOR);

    env.fromSource(
            aKafkaSource(parameters.getRequired("input-topics").split(TOPIC_PARAM_SEPARATOR)),
            noWatermarks(),
            "kafka-source")
        .uid("kafka-source")
        .keyBy(InputEvent::getId)
        .process(new StatefulCounter())
        .name("process")
        .uid("process")
        .sinkTo(
            KafkaSink.<OutputEvent>builder()
                .setRecordSerializer(new MultiTopicSerializationSchema(topics))
                .setKafkaProducerConfig(fromResource("kafka-producer.conf").toProperties())
                .setBootstrapServers("kafka-host:9092")
                .build()
        )
        .name("kafka-sink")
        .uid("kafka-sink");

    String jobName = parameters.get("job-name", "flink_multi_topic_sink");
    env.registerJobListener(
        OpenLineageFlinkJobListenerBuilder.create()
            .executionEnvironment(env)
            .jobName(jobName)
            .build());
    env.execute(jobName);
  }
}
