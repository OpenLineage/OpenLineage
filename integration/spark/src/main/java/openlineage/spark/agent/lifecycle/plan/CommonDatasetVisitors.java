package openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import openlineage.spark.agent.OpenLineageContext;
import openlineage.spark.agent.lifecycle.plan.visitor.BigQueryNodeVisitor;
import openlineage.spark.agent.lifecycle.plan.visitor.CommandPlanVisitor;
import openlineage.spark.agent.lifecycle.plan.visitor.DatasetSourceVisitor;
import openlineage.spark.agent.lifecycle.plan.visitor.LogicalRDDVisitor;
import openlineage.spark.agent.lifecycle.plan.visitor.LogicalRelationVisitor;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

public class CommonDatasetVisitors
    implements Supplier<List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>>> {

  private final SQLContext sqlContext;
  private final OpenLineageContext sparkContext;

  public CommonDatasetVisitors(SQLContext sqlContext, OpenLineageContext sparkContext) {
    this.sqlContext = sqlContext;
    this.sparkContext = sparkContext;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> get() {
    List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> list = new ArrayList<>();
    list.add(new LogicalRelationVisitor(sqlContext.sparkContext(), sparkContext.getJobNamespace()));
    list.add(new DatasetSourceVisitor());
    list.add(new LogicalRDDVisitor());
    list.add(new CommandPlanVisitor(new ArrayList<>(list)));
    if (BigQueryNodeVisitor.hasBigQueryClasses()) {
      list.add(new BigQueryNodeVisitor(sqlContext));
    }
    return list;
  }
}
