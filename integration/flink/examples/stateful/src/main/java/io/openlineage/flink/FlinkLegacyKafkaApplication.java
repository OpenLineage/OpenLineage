/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.util.FlinkListenerUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static io.openlineage.kafka.KafkaClientProvider.legacyKafkaSink;
import static io.openlineage.kafka.KafkaClientProvider.legacyKafkaSource;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

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


    env.registerJobListener(FlinkListenerUtils.instantiate(env));
    env.execute("flink-legacy-stateful");
  }
}
