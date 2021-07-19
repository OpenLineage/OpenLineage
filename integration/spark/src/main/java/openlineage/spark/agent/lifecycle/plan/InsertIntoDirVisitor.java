package openlineage.spark.agent.lifecycle.plan;

import java.util.Collections;
import java.util.List;

import openlineage.spark.agent.client.LineageEvent;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoDir;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDir} and extracts the output {@link
 * LineageEvent.Dataset} being written.
 */
public class InsertIntoDirVisitor extends AbstractPartialFunction<LogicalPlan, List<LineageEvent.Dataset>> {
  private final SQLContext sqlContext;

  public InsertIntoDirVisitor(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof InsertIntoDir;
  }

  @Override
  public List<LineageEvent.Dataset> apply(LogicalPlan x) {
    InsertIntoDir cmd = (InsertIntoDir) x;
    CatalogStorageFormat storage = cmd.storage();
    return ScalaConversionUtils.asJavaOptional(storage.locationUri())
        .map(
            uri -> {
              Path path = new Path(uri);
              if (uri.getScheme() == null) {
                path = new Path("file", null, uri.toString());
              }
              return Collections.singletonList(
                  PlanUtils.getDataset(path.toUri(), cmd.child().schema()));
            })
        .orElse(Collections.emptyList());
  }
}
