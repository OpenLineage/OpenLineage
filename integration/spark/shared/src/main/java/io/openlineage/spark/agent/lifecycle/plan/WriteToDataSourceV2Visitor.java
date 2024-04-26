/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite;
import org.jetbrains.annotations.NotNull;
import scala.Option;

@Slf4j
public final class WriteToDataSourceV2Visitor
    extends QueryPlanVisitor<WriteToDataSourceV2, OutputDataset> {
  private static final String KAFKA_STREAMING_WRITE_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaStreamingWrite";

  public WriteToDataSourceV2Visitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    boolean result = plan instanceof WriteToDataSourceV2;
    if (log.isDebugEnabled()) {
      log.debug(
          "The supplied logical plan {} an instance of {}",
          result ? "IS" : "IS NOT",
          WriteToDataSourceV2.class.getCanonicalName());
    }
    return result;
  }

  @Override
  public List<OutputDataset> apply(LogicalPlan plan) {
    WriteToDataSourceV2 write = (WriteToDataSourceV2) plan;
    BatchWrite batchWrite = write.batchWrite();
    if (batchWrite instanceof MicroBatchWrite) {
      MicroBatchWrite microBatchWrite = (MicroBatchWrite) batchWrite;
      StreamingWrite streamingWrite = microBatchWrite.writeSupport();
      Class<? extends StreamingWrite> streamingWriteClass = streamingWrite.getClass();
      String streamingWriteClassName = streamingWriteClass.getCanonicalName();
      if (KAFKA_STREAMING_WRITE_CLASS_NAME.equals(streamingWriteClassName)) {
        return handleKafkaStreamingWrite(streamingWrite, write);
      }
    }

    return Collections.emptyList();
  }

  private @NotNull List<OutputDataset> handleKafkaStreamingWrite(
      StreamingWrite streamingWrite, WriteToDataSourceV2 write) {
    KafkaStreamWriteProxy proxy = new KafkaStreamWriteProxy(streamingWrite);
    Optional<String> topicOpt = proxy.getTopic();
    Optional<String> bootstrapServersOpt = proxy.getBootstrapServers();
    if (topicOpt.isPresent() && bootstrapServersOpt.isPresent()) {
      String topic = topicOpt.get();
      String bootstrapServers = bootstrapServersOpt.get();
      OutputDataset dataset =
          outputDataset().getDataset(topic, "kafka://" + bootstrapServers, write.schema());
      return Collections.singletonList(dataset);
    } else {
      String topicPresent =
          topicOpt.isPresent() ? "Topic **IS** present" : "Topic **IS NOT** present";
      String bootstrapServersPresent =
          bootstrapServersOpt.isPresent()
              ? "Bootstrap servers **IS** present"
              : "Bootstrap servers **IS NOT** present";
      log.warn(
          "Both topic and bootstrapServers need to be present in order to construct an output dataset. {}. {}",
          bootstrapServersPresent,
          topicPresent);
      return Collections.emptyList();
    }
  }

  @Slf4j
  private static final class KafkaStreamWriteProxy {
    private final StreamingWrite streamingWrite;

    public KafkaStreamWriteProxy(StreamingWrite streamingWrite) {
      String incomingClassName = streamingWrite.getClass().getCanonicalName();
      if (!KAFKA_STREAMING_WRITE_CLASS_NAME.equals(incomingClassName)) {
        throw new IllegalArgumentException(
            "Expected the supplied argument to be of type '"
                + KAFKA_STREAMING_WRITE_CLASS_NAME
                + "' but received '"
                + incomingClassName
                + "' instead");
      }

      this.streamingWrite = streamingWrite;
    }

    public Optional<String> getTopic() {
      return this.<Option<String>>tryReadField(streamingWrite, "topic")
          .flatMap(opt -> Optional.ofNullable(opt.isDefined() ? opt.get() : null));
    }

    public Optional<String> getBootstrapServers() {
      Optional<Map<String, Object>> producerParams = tryReadField(streamingWrite, "producerParams");
      return producerParams.flatMap(
          props -> Optional.ofNullable((String) props.get("bootstrap.servers")));
    }

    private <T> Optional<T> tryReadField(Object target, String fieldName) {
      try {
        T result = (T) FieldUtils.readDeclaredField(target, fieldName, true);
        return result == null ? Optional.empty() : Optional.of(result);
      } catch (IllegalAccessException e) {
        return Optional.empty();
      }
    }
  }
}
