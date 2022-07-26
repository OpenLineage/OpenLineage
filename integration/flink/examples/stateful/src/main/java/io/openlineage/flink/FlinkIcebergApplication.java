/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;

import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class FlinkIcebergApplication {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = setupEnv(args);

    TableLoader sourceLoader = TableLoader.fromHadoopTable("/tmp/warehouse/db/source");
    TableLoader sinkLoader = TableLoader.fromHadoopTable("/tmp/warehouse/db/sink");

    DataStream<RowData> stream = FlinkSource.forRowData()
      .env(env)
      .tableLoader(sourceLoader)
      .streaming(true)
      .build();

    FlinkSink.forRowData(stream)
      .tableLoader(sinkLoader)
      .overwrite(true)
      .append();

    // we use this app to test open lineage flink integration so it cannot make use of OpenLineageFlinkJobListener classes
    JobListener openlineageJobListener = (JobListener) Class.forName("io.openlineage.flink.OpenLineageFlinkJobListener")
      .getConstructor(StreamExecutionEnvironment.class)
      .newInstance(env);

    env.registerJobListener(openlineageJobListener);
    env.execute("flink-examples-iceberg");
  }
}
