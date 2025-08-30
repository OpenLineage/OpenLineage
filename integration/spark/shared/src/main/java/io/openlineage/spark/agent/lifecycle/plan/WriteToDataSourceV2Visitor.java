/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.Naming;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.net.URI;
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
import org.apache.spark.sql.types.StructType;
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
          "The supplied logical plan {} {} an instance of {}",
          plan.getClass().getCanonicalName(),
          result ? "IS" : "IS NOT",
          WriteToDataSourceV2.class.getCanonicalName());
    }
    return result;
  }

  @Override
  public List<OutputDataset> apply(LogicalPlan plan) {
    List<OutputDataset> result = Collections.emptyList();
    WriteToDataSourceV2 write = (WriteToDataSourceV2) plan;
    BatchWrite batchWrite = write.batchWrite();
    if (batchWrite instanceof MicroBatchWrite) {
      MicroBatchWrite microBatchWrite = (MicroBatchWrite) batchWrite;
      StreamingWrite streamingWrite = microBatchWrite.writeSupport();
      Class<? extends StreamingWrite> streamingWriteClass = streamingWrite.getClass();
      String streamingWriteClassName = streamingWriteClass.getCanonicalName();
      if (KAFKA_STREAMING_WRITE_CLASS_NAME.equals(streamingWriteClassName)) {
        result = handleKafkaStreamingWrite(streamingWrite);
      } else {
        log.warn(
            "The streaming write class '{}' for '{}' is not supported",
            streamingWriteClass,
            MicroBatchWrite.class.getCanonicalName());
      }
    } else {
      log.warn("Unsupported batch write class: {}", batchWrite.getClass().getCanonicalName());
    }

    return result;
  }

  private @NotNull List<OutputDataset> handleKafkaStreamingWrite(StreamingWrite streamingWrite) {
    KafkaStreamWriteProxy proxy = new KafkaStreamWriteProxy(streamingWrite);
    Optional<String> topicOpt = proxy.getTopic();
    StructType schemaOpt = proxy.getSchema();

    Optional<String> bootstrapServersOpt = proxy.getBootstrapServers();
    URI namespace = KafkaBootstrapServerResolver.resolve(bootstrapServersOpt);

    if (topicOpt.isPresent() && bootstrapServersOpt.isPresent()) {
      String topic = topicOpt.get();
      Naming.Kafka name =
          new Naming.Kafka(namespace.getHost(), Integer.toString(namespace.getPort()), topic);
      OutputDataset dataset = outputDataset().getDataset(name, schemaOpt);
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
          .flatMap(ScalaConversionUtils::asJavaOptional);
    }

    public StructType getSchema() {
      Optional<StructType> schema = this.tryReadField(streamingWrite, "schema");
      return schema.orElseGet(StructType::new);
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
