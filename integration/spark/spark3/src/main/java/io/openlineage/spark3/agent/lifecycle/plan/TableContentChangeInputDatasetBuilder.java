/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DeleteUpdateCommandUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;

/**
 * Extracts input datasets from the subqueries of a {@link DeleteFromTable}, an {@link UpdateTable}
 * or of the Databricks Delta command the same statement is resolved to.
 *
 * <p>Tables referenced from a WHERE clause or from an UPDATE assignment live inside expressions,
 * not inside the children of the plan node:
 *
 * <pre>
 * DeleteFromTable
 * +- table: DataSourceV2Relation           (visited, emitted as output)
 * +- condition: InSubquery(id, ListQuery)  (not a child, never visited)
 *                +- plan: the subquery reading another table
 * </pre>
 *
 * The regular traversal only walks children, so those tables were missing from the event. {@code
 * subqueries()} is available on the Databricks commands as well, because the condition is one of
 * their expressions.
 */
public class TableContentChangeInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public TableContentChangeInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof DeleteFromTable)
        || (x instanceof UpdateTable)
        || DeleteUpdateCommandUtils.isDeleteOrUpdateCommand(x);
  }

  @Override
  public List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    // subqueries() returns the plans held by the expressions of this node only, so the children of
    // the node are left to the regular traversal
    return ScalaConversionUtils.<LogicalPlan>fromSeq(x.subqueries()).stream()
        .flatMap(subquery -> collectInputs(subquery, event).stream())
        .collect(Collectors.toList());
  }

  private List<InputDataset> collectInputs(LogicalPlan subquery, SparkListenerEvent event) {
    return ScalaConversionUtils.fromSeq(
            subquery.collect(
                delegate(
                    context.getInputDatasetQueryPlanVisitors(),
                    context.getInputDatasetBuilders(),
                    event)))
        .stream()
        .flatMap(datasets -> datasets.stream())
        .collect(Collectors.toList());
  }
}
