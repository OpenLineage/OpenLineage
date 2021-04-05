package marquez.spark.agent.lifecycle.plan;

import java.util.List;
import marquez.spark.agent.client.LineageEvent.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import scala.Option;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link SaveIntoDataSourceCommand} and extracts the
 * output {@link Dataset} being written. Since the output datasource is a {@link BaseRelation}, we
 * wrap it with an artificial {@link LogicalRelation} so we can delegate to other plan visitors.
 */
public class SaveIntoDataSourceCommandVisitor
    extends AbstractPartialFunction<LogicalPlan, List<Dataset>> {
  private final SQLContext sqlContext;
  private final List<PartialFunction<LogicalPlan, List<Dataset>>> relationVisitors;

  public SaveIntoDataSourceCommandVisitor(
      SQLContext sqlContext, List<PartialFunction<LogicalPlan, List<Dataset>>> relationVisitors) {
    this.sqlContext = sqlContext;
    this.relationVisitors = relationVisitors;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof SaveIntoDataSourceCommand
        && ((SaveIntoDataSourceCommand) x).dataSource() instanceof SchemaRelationProvider;
  }

  @Override
  public List<Dataset> apply(LogicalPlan x) {
    SchemaRelationProvider p =
        (SchemaRelationProvider) ((SaveIntoDataSourceCommand) x).dataSource();
    BaseRelation relation =
        p.createRelation(sqlContext, ((SaveIntoDataSourceCommand) x).options(), x.schema());
    return PlanUtils.applyFirst(
        relationVisitors,
        new LogicalRelation(
            relation, relation.schema().toAttributes(), Option.empty(), x.isStreaming()));
  }
}
