/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static io.openlineage.flink.StreamEnvironment.setupEnv;

public class FlinkFakeApplication {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = setupEnv(args);

    env.addSource(new FakeSource()).addSink(new FakeSink());

    // we use this app to test open lineage flink integration so it cannot make use of OpenLineageFlinkJobListener classes
    JobListener openlineageJobListener = (JobListener) Class.forName("io.openlineage.flink.OpenLineageFlinkJobListener")
      .getConstructor(StreamExecutionEnvironment.class)
      .newInstance(env);

    env.registerJobListener(openlineageJobListener);
    env.execute("flink-fake-application");
  }

  static class FakeSource implements SourceFunction<Integer> {
    boolean isRunning = true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
      while(isRunning) {
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(1);
        }
        Thread.sleep(100);
      }
    }

    @Override
    public void cancel() {
      isRunning = true;
    }
  }

  static class FakeSink implements SinkFunction<Integer> {}
}
