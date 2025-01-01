/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;

@Slf4j
public class StreamingDataSourceV2RelationVisitor
    extends QueryPlanVisitor<StreamingDataSourceV2Relation, InputDataset> {
  private static final String KAFKA_MICRO_BATCH_STREAM_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaMicroBatchStream";
  private static final String KINESIS_MICRO_BATCH_STREAM_CLASS_NAME =
      "org.apache.spark.sql.connector.kinesis.KinesisV2MicrobatchStream";
  private static final String MONGO_MICRO_BATCH_STREAM_CLASS_NAME =
      "com.mongodb.spark.sql.connector.read.MongoMicroBatchStream";

  public StreamingDataSourceV2RelationVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<InputDataset> apply(LogicalPlan x) {
    log.info(
        "Applying {} to a logical plan with type {}",
        this.getClass().getSimpleName(),
        x.getClass().getCanonicalName());
    final StreamingDataSourceV2Relation relation = (StreamingDataSourceV2Relation) x;
    final StreamStrategy streamStrategy = selectStrategy(relation);
    return streamStrategy.getInputDatasets();
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    boolean result = x instanceof StreamingDataSourceV2Relation;
    if (log.isDebugEnabled()) {
      log.debug(
          "The result of checking whether {} is an instance of {} is {}",
          x.getClass().getCanonicalName(),
          StreamingDataSourceV2Relation.class.getCanonicalName(),
          result);
    }
    return result;
  }

  public StreamStrategy selectStrategy(StreamingDataSourceV2Relation relation) {
    StreamStrategy streamStrategy;
    Class<?> streamClass = relation.stream().getClass();
    String streamClassName = streamClass.getCanonicalName();
    if (KAFKA_MICRO_BATCH_STREAM_CLASS_NAME.equals(streamClassName)) {
      streamStrategy =
          new KafkaMicroBatchStreamStrategy(
              inputDataset(),
              relation.schema(),
              relation.stream(),
              ScalaConversionUtils.asJavaOptional(relation.startOffset()));
    } else if (KINESIS_MICRO_BATCH_STREAM_CLASS_NAME.equals(streamClassName)) {
      streamStrategy = new KinesisMicroBatchStreamStrategy(inputDataset(), relation);
    } else if (MONGO_MICRO_BATCH_STREAM_CLASS_NAME.equals(streamClassName)) {
      streamStrategy = new MongoMicroBatchStreamStrategy(inputDataset(), relation);
    } else {
      log.warn(
          "The {} has been selected because no rules have matched for the stream class of {}",
          NoOpStreamStrategy.class,
          streamClassName);
      streamStrategy =
          new NoOpStreamStrategy(
              inputDataset(),
              relation.schema(),
              relation.stream(),
              ScalaConversionUtils.asJavaOptional(relation.startOffset()));
    }

    log.info("Selected this strategy: {}", streamStrategy.getClass().getSimpleName());
    return streamStrategy;
  }
}
