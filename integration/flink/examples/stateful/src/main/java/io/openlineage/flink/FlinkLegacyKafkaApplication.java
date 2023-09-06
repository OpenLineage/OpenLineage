/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static io.openlineage.kafka.KafkaClientProvider.legacyKafkaSink;
import static io.openlineage.kafka.KafkaClientProvider.legacyKafkaSource;

public class FlinkLegacyKafkaApplication {

  private static final String TOPIC_PARAM_SEPARATOR = ",";

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = setupEnv(args);

    SourceFunction<InputEvent> source = legacyKafkaSource(parameters.getRequired("input-topics").split(TOPIC_PARAM_SEPARATOR));
    env.addSource(source, "kafka-source")
      .keyBy(InputEvent::getId)
      .process(new StatefulCounter()).name("process").uid("process")
      .addSink(legacyKafkaSink(parameters.getRequired("output-topic"))).name("kafka-sink").uid("kafka-sink");

    String jobName = parameters.get("job-name", "flink_legacy_stateful");
    env.registerJobListener(
        OpenLineageFlinkJobListenerBuilder
            .create()
            .executionEnvironment(env)
            .jobName(jobName)
            .build()
    );
    env.execute(jobName);
  }
}
