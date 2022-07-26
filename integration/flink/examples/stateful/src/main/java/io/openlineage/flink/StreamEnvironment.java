/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class StreamEnvironment {
  public static StreamExecutionEnvironment setupEnv(String[] args) {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.getConfig().setGlobalJobParameters(parameters);
    env.setParallelism(parameters.getInt("parallelism", 1));
    env.enableCheckpointing(parameters.getInt("checkpoint.interval", 1_000), CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameters.getInt("min.pause.between.checkpoints", 1_000));
    env.getCheckpointConfig().setCheckpointTimeout(parameters.getInt("checkpoint.timeout", 90_000));

    env.setRestartStrategy(RestartStrategies.noRestart());
    return env;
  }
}
