/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.lifecycle.plan.KafkaMicroBatchStreamStrategy;
import io.openlineage.spark.agent.lifecycle.plan.NoOpStreamStrategy;
import io.openlineage.spark.agent.lifecycle.plan.StreamStrategy;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation;

@Slf4j
public class StreamingDataSourceV2ScanRelationDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<StreamingDataSourceV2ScanRelation> {

  private static final String KAFKA_MICRO_BATCH_STREAM_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaMicroBatchStream";

  public StreamingDataSourceV2ScanRelationDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof StreamingDataSourceV2ScanRelation;
  }

  @Override
  protected List<InputDataset> apply(
      SparkListenerEvent event, StreamingDataSourceV2ScanRelation relation) {
    StreamStrategy streamStrategy;
    Class<?> streamClass = relation.stream().getClass();
    String streamClassName = streamClass.getCanonicalName();

    if (KAFKA_MICRO_BATCH_STREAM_CLASS_NAME.equals(streamClassName)) {
      streamStrategy =
          new KafkaMicroBatchStreamStrategy(
              inputDataset(),
              relation.relation().schema(),
              relation.stream(),
              ScalaConversionUtils.<Offset>asJavaOptional(relation.startOffset()));
    } else {
      log.warn(
          "The {} has been selected because no rules have matched for the stream class of {}",
          NoOpStreamStrategy.class,
          streamClassName);
      streamStrategy =
          new NoOpStreamStrategy(
              inputDataset(),
              relation.relation().schema(),
              relation.stream(),
              ScalaConversionUtils.<Offset>asJavaOptional(relation.startOffset()));
    }

    log.info("Selected this strategy: {}", streamStrategy.getClass().getSimpleName());
    return streamStrategy.getInputDatasets();
  }
}
