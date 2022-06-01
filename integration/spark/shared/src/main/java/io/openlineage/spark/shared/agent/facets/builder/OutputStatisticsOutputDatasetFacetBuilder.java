/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.facets.builder;

import io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet;
import io.openlineage.spark.shared.agent.JobMetricsHolder;
import io.openlineage.spark.shared.api.CustomFacetBuilder;
import io.openlineage.spark.shared.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerJobEnd;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Write {@link OutputStatisticsOutputDatasetFacet} if statistics are present in the job metrics.
 */
public class OutputStatisticsOutputDatasetFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobEnd, OutputStatisticsOutputDatasetFacet> {

  private final JobMetricsHolder jobMetricsHolder = JobMetricsHolder.getInstance();
  private final OpenLineageContext context;

  public OutputStatisticsOutputDatasetFacetBuilder(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  protected void build(
      SparkListenerJobEnd event,
      BiConsumer<String, ? super OutputStatisticsOutputDatasetFacet> consumer) {
    Map<JobMetricsHolder.Metric, Number> metrics = jobMetricsHolder.pollMetrics(event.jobId());

    if (metrics.containsKey(JobMetricsHolder.Metric.WRITE_BYTES)
        || metrics.containsKey(JobMetricsHolder.Metric.WRITE_RECORDS)) {
      consumer.accept(
          "outputStatistics",
          context
              .getOpenLineage()
              .newOutputStatisticsOutputDatasetFacet(
                  Optional.of(metrics.get(JobMetricsHolder.Metric.WRITE_RECORDS))
                      .map(Number::longValue)
                      .orElse(null),
                  Optional.of(metrics.get(JobMetricsHolder.Metric.WRITE_BYTES))
                      .map(Number::longValue)
                      .orElse(null)));
    }
  }
}
