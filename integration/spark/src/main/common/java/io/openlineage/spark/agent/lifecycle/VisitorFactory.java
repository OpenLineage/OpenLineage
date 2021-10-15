package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import java.util.List;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Provides Visitors for iterating on {@link LogicalPlan}.
 *
 * <p>All common Visitors would be grouped and passed to {@link
 * VisitorFactory#getOutputVisitors(SQLContext, String)} to retrieve Visitors for {@link
 * OpenLineage.OutputDataset}
 */
interface VisitorFactory {

  List<QueryPlanVisitor<LogicalPlan, OpenLineage.InputDataset>> getInputVisitors(
      SQLContext sqlContext, String jobNamespace);

  List<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>> getOutputVisitors(
      SQLContext sqlContext, String jobNamespace);
}
