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

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class FlinkFailedApplication {

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.getConfig().setGlobalJobParameters(parameters);
    env.setParallelism(parameters.getInt("parallelism", 1));
    env.enableCheckpointing(parameters.getInt("checkpoint.interval", 1_000), CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameters.getInt("min.pause.between.checkpoints", 1_000));
    env.getCheckpointConfig().setCheckpointTimeout(parameters.getInt("checkpoint.timeout", 90_000));

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
