package io.openlineage.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;

import static io.openlineage.flink.StreamEnvironment.setupEnv;


public class FlinkFailedApplication {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = setupEnv(args);

    env.setRestartStrategy(RestartStrategies.noRestart());

    TableLoader sourceLoader = TableLoader.fromHadoopTable("/tmp/warehouse/db/source");
    TableLoader sinkLoader = TableLoader.fromHadoopTable("/tmp/warehouse/db/sink");

    DataStream<RowData> stream = FlinkSource.forRowData()
      .env(env)
      .tableLoader(sourceLoader)
      .streaming(true)
      .build();

    DataStream<RowData> failedTransform = stream.map(
      row -> {
        throw new RuntimeException("fail");
      }
    );

    FlinkSink.forRowData(failedTransform)
      .tableLoader(sinkLoader)
      .overwrite(true)
      .append();

    // we use this app to test open lineage flink integration so it cannot make use of OpenLineageFlinkJobListener classes
    JobListener openlineageJobListener = (JobListener) Class.forName("io.openlineage.flink.OpenLineageFlinkJobListener")
      .getConstructor(StreamExecutionEnvironment.class)
      .newInstance(env);

    env.registerJobListener(openlineageJobListener);
    env.execute("flink-failed-job");
  }
}
