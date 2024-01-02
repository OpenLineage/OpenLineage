/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;

import java.time.Duration;

import static io.openlineage.flink.StreamEnvironment.setupEnv;

public class FlinkIcebergSourceApplication {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = setupEnv(args);

    TableLoader sourceLoader = TableLoader.fromHadoopTable("/tmp/warehouse/db/source");
    TableLoader sinkLoader = TableLoader.fromHadoopTable("/tmp/warehouse/db/sink");

    DataStream<RowData> stream = env.fromSource(
            IcebergSource
                    .forRowData()
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .tableLoader(sourceLoader)
                    .streaming(true)
                    .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
                    .monitorInterval(Duration.ofMinutes(1))
                    .build(),
            WatermarkStrategy.noWatermarks(),
            "iceberg-source",
            TypeInformation.of(RowData.class));

    FlinkSink.forRowData(stream)
      .tableLoader(sinkLoader)
      .overwrite(true)
      .append();

    env.registerJobListener(
        OpenLineageFlinkJobListenerBuilder
            .create()
            .executionEnvironment(env)
            .jobName("flink_examples_iceberg_source")
            .build()
    );
    env.execute("flink_examples_iceberg_source");
  }
}
