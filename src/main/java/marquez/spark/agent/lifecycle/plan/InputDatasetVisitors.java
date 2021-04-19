package marquez.spark.agent.lifecycle.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import marquez.spark.agent.client.LineageEvent.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Constructs a list of valid {@link LogicalPlan} visitors that can extract an input {@link
 * Dataset}. Checks the classpath for classes that are not bundled with Spark to avoid {@link
 * ClassNotFoundException}s during plan traversal.
 */
public class InputDatasetVisitors
    implements Supplier<List<PartialFunction<LogicalPlan, List<Dataset>>>> {
  private final SQLContext sqlContext;

  public InputDatasetVisitors(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<Dataset>>> get() {
    List<PartialFunction<LogicalPlan, List<Dataset>>> list = new ArrayList<>();
    list.add(new HadoopFsRelationVisitor(sqlContext.sparkContext()));
    list.add(new JDBCRelationVisitor(sqlContext));
    list.add(new DatasetSourceVisitor());
    list.add(new CommandPlanVisitor(new ArrayList<>(list)));
    return list;
  }
}
