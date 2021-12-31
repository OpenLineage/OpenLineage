package io.openlineage.spark3.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.OVERWRITE;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression;
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class TableContentChangeVisitor
    extends QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset> {

  public TableContentChangeVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return (x instanceof OverwriteByExpression)
        || (x instanceof OverwritePartitionsDynamic)
        || (x instanceof InsertIntoStatement);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    NamedRelation table;
    TableStateChangeFacet overwriteFacet = new TableStateChangeFacet(OVERWRITE);
    Map<String, OpenLineage.DefaultDatasetFacet> facetMap = new HashMap<>();

    // INSERT OVERWRITE TABLE SQL statement is translated into InsertIntoTable logical operator.
    if (x instanceof OverwriteByExpression) {
      table = ((OverwriteByExpression) x).table();
      facetMap.put("tableStateChange", overwriteFacet);
    } else if (x instanceof InsertIntoStatement) {
      table = (NamedRelation) ((InsertIntoStatement) x).table();
      if (((InsertIntoStatement) x).overwrite()) {
        facetMap.put("tableStateChange", overwriteFacet);
      }
    } else {
      table = ((OverwritePartitionsDynamic) x).table();
      facetMap.put("tableStateChange", overwriteFacet);
    }

    return PlanUtils3.fromDataSourceV2Relation(
        outputDataset(), context, (DataSourceV2Relation) table, facetMap);
  }
}
