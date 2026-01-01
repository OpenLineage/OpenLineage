/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

public class StreamEnvironment {
  public static StreamExecutionEnvironment setupEnv(String... args) {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.getConfig().setGlobalJobParameters(parameters);
    env.setParallelism(parameters.getInt("parallelism", 1));
    env.enableCheckpointing(
        parameters.getInt("checkpoint.interval", 1_000), CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setExternalizedCheckpointRetention(RETAIN_ON_CANCELLATION);
    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(parameters.getInt("min.pause.between.checkpoints", 1_000));
    env.getCheckpointConfig().setCheckpointTimeout(parameters.getInt("checkpoint.timeout", 90_000));

    return env;
  }
}
