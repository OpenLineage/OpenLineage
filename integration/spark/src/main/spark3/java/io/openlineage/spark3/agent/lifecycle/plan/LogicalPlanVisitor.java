package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

public class LogicalPlanVisitor extends QueryPlanVisitor<LogicalRelation> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    LogicalRelation logRel = (LogicalRelation) x;
    String text = logRel.simpleString(Integer.MAX_VALUE);
    return Collections.emptyList();
  }
}
