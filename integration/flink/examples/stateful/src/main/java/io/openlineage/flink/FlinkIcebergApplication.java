/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.util.FlinkListenerUtils;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;

import static io.openlineage.flink.StreamEnvironment.setupEnv;

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

    env.registerJobListener(FlinkListenerUtils.instantiate(env));
    env.execute("flink-examples-iceberg");
  }
}
