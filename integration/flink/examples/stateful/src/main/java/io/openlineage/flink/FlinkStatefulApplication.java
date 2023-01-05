/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.flink.avro.event.InputEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static io.openlineage.kafka.KafkaClientProvider.aKafkaSink;
import static io.openlineage.kafka.KafkaClientProvider.aKafkaSource;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class FlinkStatefulApplication {

    private static final String TOPIC_PARAM_SEPARATOR = ",";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = setupEnv(args);

        env.fromSource(aKafkaSource(parameters.getRequired("input-topics").split(TOPIC_PARAM_SEPARATOR)), noWatermarks(), "kafka-source").uid("kafka-source")
                .keyBy(InputEvent::getId)
                .process(new StatefulCounter()).name("process").uid("process")
                .sinkTo(aKafkaSink(parameters.getRequired("output-topic"))).name("kafka-sink").uid("kafka-sink");


        // we use this app to test open lineage flink integration so it cannot make use of OpenLineageFlinkJobListener classes
        JobListener openlineageJobListener = (JobListener) Class.forName("io.openlineage.flink.OpenLineageFlinkJobListener")
                .getConstructor(StreamExecutionEnvironment.class)
                    .newInstance(env);

        env.registerJobListener(openlineageJobListener);
        env.execute("flink-examples-stateful");
    }
}
