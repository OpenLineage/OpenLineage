/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.LogicalPlanFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

/**
 * {@link CustomFacetBuilder} that generates a {@link LogicalPlanFacet} for each {@link
 * SparkListenerSQLExecutionStart}, {@link SparkListenerSQLExecutionEnd}, and {@link
 * SparkListenerJobEnd} event if a {@link org.apache.spark.sql.execution.QueryExecution} is present.
 */
public class LogicalPlanRunFacetBuilder extends CustomFacetBuilder<Object, LogicalPlanFacet> {
  private final OpenLineageContext openLineageContext;

  public LogicalPlanRunFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return (x instanceof SparkListenerSQLExecutionEnd
            || x instanceof SparkListenerSQLExecutionStart
            || x instanceof SparkListenerJobEnd)
        && openLineageContext.getQueryExecution().isPresent();
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super LogicalPlanFacet> consumer) {
    openLineageContext
        .getQueryExecution()
        .ifPresent(
            qe ->
                consumer.accept(
                    "spark.logicalPlan",
                    LogicalPlanFacet.builder().plan(qe.optimizedPlan()).build()));
  }
}
