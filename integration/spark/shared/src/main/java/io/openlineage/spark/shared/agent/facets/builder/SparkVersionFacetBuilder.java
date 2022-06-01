/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.facets.builder;

import io.openlineage.spark.shared.agent.facets.SparkVersionFacet;
import io.openlineage.spark.shared.api.CustomFacetBuilder;
import io.openlineage.spark.shared.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerEvent;

import java.util.function.BiConsumer;

/**
 * {@link CustomFacetBuilder} that adds the {@link SparkVersionFacet} to a run. This facet is
 * generated for every {@link SparkListenerEvent}.
 */
public class SparkVersionFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, SparkVersionFacet> {
  private final OpenLineageContext openLineageContext;

  public SparkVersionFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super SparkVersionFacet> consumer) {
    consumer.accept("spark_version", new SparkVersionFacet(openLineageContext.getSparkContext()));
  }
}
