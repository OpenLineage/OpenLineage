package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.spark.agent.util.PlanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelectStatement;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Collections;

@Slf4j
public class CreateTableAsSelectStatementVisitor extends 
    QueryPlanVisitor<CreateTableAsSelectStatement, OpenLineage.Dataset> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    log.warn(x.toJSON());
    if (x instanceof CreateTableAsSelectStatement) {
      return true;
    }
    return false;
  }


  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CreateTableAsSelectStatement statement = (CreateTableAsSelectStatement) x;
    log.info("STS");
    log.warn(statement.toString());
    
//    Path path;
//    if (statement.location().isDefined()) {
//      path = new Path(statement.location().get());
//    }
//    else if (statement.provider().isDefined()){
//      path = new Path(String.format("%s://%s", statement.provider().isDefined()))
//    }
    
    return Collections.singletonList(
      PlanUtils.getDataset(
        statement.tableName().mkString("."),
        statement.provider().get(),
        PlanUtils.datasetFacet(statement.schema(), statement.provider().get())));
  }
}
