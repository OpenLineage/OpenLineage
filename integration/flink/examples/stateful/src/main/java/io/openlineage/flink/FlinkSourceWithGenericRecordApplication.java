/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.common.config.ConfigWrapper.fromResource;
import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static io.openlineage.kafka.KafkaClientProvider.aKafkaSink;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
public class FlinkSourceWithGenericRecordApplication {

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = setupEnv(args);

    KafkaSourceBuilder<GenericRecord> builder =
        KafkaSource.<GenericRecord>builder()
            .setProperties(fromResource("kafka-consumer.conf").toProperties())
            .setBootstrapServers("kafka:9092")
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forGeneric(InputEvent.getClassSchema()));

    builder.setTopics("io.openlineage.flink.kafka.input_no_schema_registry");

    KafkaSource<GenericRecord> kafkaSource = builder.build();

    env.fromSource(kafkaSource, noWatermarks(), "kafka-source")
        .uid("kafka-source")
        .map(record -> new InputEvent((String) record.get("id"), (Long) record.get("version")))
        .keyBy(InputEvent::getId)
        .process(new StatefulCounter())
        .name("process")
        .uid("process")
        .sinkTo(aKafkaSink(parameters.getRequired("output-topics")))
        .name("kafka-sink")
        .uid("kafka-sink");

    String jobName = parameters.get("job-name", "flink_source_with_generic_record");
    env.registerJobListener(
        OpenLineageFlinkJobListenerBuilder.create()
            .executionEnvironment(env)
            .jobName(jobName)
            .build());
    env.execute(jobName);
  }
}
