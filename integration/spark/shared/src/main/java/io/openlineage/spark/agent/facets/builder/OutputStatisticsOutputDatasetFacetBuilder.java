/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * Write {@link OutputStatisticsOutputDatasetFacet} if statistics are present in the job metrics.
 */
@Slf4j
public class OutputStatisticsOutputDatasetFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, OutputStatisticsOutputDatasetFacet> {

  private final JobMetricsHolder jobMetricsHolder = JobMetricsHolder.getInstance();
  private final OpenLineageContext context;

  public OutputStatisticsOutputDatasetFacetBuilder(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return (x instanceof SparkListenerJobEnd) || (x instanceof SparkListenerSQLExecutionEnd);
  }

  @Override
  protected void build(
      SparkListenerEvent event,
      BiConsumer<String, ? super OutputStatisticsOutputDatasetFacet> consumer) {
    if (!context.getActiveJobId().isPresent()) {
      log.warn("No jobId found in context");
      return;
    }

    Map<JobMetricsHolder.Metric, Number> metrics =
        jobMetricsHolder.pollMetrics(context.getActiveJobId().get());
    if (metrics.containsKey(JobMetricsHolder.Metric.WRITE_BYTES)
        || metrics.containsKey(JobMetricsHolder.Metric.WRITE_RECORDS)) {
      consumer.accept(
          "outputStatistics",
          context
              .getOpenLineage()
              .newOutputStatisticsOutputDatasetFacetBuilder()
              .rowCount(
                  Optional.of(metrics.get(JobMetricsHolder.Metric.WRITE_RECORDS))
                      .map(Number::longValue)
                      .orElse(null))
              .size(
                  Optional.of(metrics.get(JobMetricsHolder.Metric.WRITE_BYTES))
                      .map(Number::longValue)
                      .orElse(null))
              .build());
    }
  }
}
