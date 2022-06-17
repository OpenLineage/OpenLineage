/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.facets.ErrorFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;

/** {@link CustomFacetBuilder} that generates an {@link ErrorFacet} for job failure events. */
public class ErrorFacetBuilder extends CustomFacetBuilder<SparkListenerJobEnd, RunFacet> {

  @Override
  public boolean isDefinedAt(Object x) {
    return super.isDefinedAt(x) && ((SparkListenerJobEnd) x).jobResult() instanceof JobFailed;
  }

  @Override
  protected void build(SparkListenerJobEnd event, BiConsumer<String, ? super RunFacet> consumer) {
    if (event.jobResult() instanceof JobFailed) {
      consumer.accept(
          "spark.exception",
          ErrorFacet.builder().exception(((JobFailed) event.jobResult()).exception()).build());
    }
  }
}
