/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite;

/**
 * Extracts relation-backed output datasets from non-Kafka V2 streaming writes on Spark 3.2 and
 * newer.
 *
 * <p>Kafka remains handled by {@code WriteToDataSourceV2Visitor}. Spark 3.1 does not expose {@link
 * WriteToDataSourceV2#relation()} and therefore does not register this builder.
 */
@Slf4j
public class WriteToDataSourceV2DatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<WriteToDataSourceV2> {

  private static final String KAFKA_STREAMING_WRITE_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaStreamingWrite";

  public WriteToDataSourceV2DatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    if (!(logicalPlan instanceof WriteToDataSourceV2)) {
      return false;
    }

    BatchWrite batchWrite = ((WriteToDataSourceV2) logicalPlan).batchWrite();
    if (!(batchWrite instanceof MicroBatchWrite)) {
      return false;
    }

    StreamingWrite streamingWrite = ((MicroBatchWrite) batchWrite).writeSupport();
    return streamingWrite != null
        && !KAFKA_STREAMING_WRITE_CLASS_NAME.equals(streamingWrite.getClass().getCanonicalName());
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, WriteToDataSourceV2 write) {
    Optional<DataSourceV2Relation> relation = ScalaConversionUtils.asJavaOptional(write.relation());
    if (!relation.isPresent()) {
      log.warn(
          "Cannot extract the output dataset for streaming write '{}' because its WriteToDataSourceV2 relation is empty",
          streamingWriteClassName(write));
      return Collections.emptyList();
    }

    // Let the relation builder preserve connector-specific outputs when an extension handles the
    // table and fall back to generic relation extraction otherwise.
    return delegate(relation.get(), event);
  }

  private String streamingWriteClassName(WriteToDataSourceV2 write) {
    BatchWrite batchWrite = write.batchWrite();
    if (!(batchWrite instanceof MicroBatchWrite)) {
      return "unknown";
    }

    StreamingWrite streamingWrite = ((MicroBatchWrite) batchWrite).writeSupport();
    return streamingWrite == null ? "unknown" : streamingWrite.getClass().getName();
  }
}
