package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoDir;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDir} and extracts the output {@link
 * OpenLineage.Dataset} being written.
 */
public class InsertIntoDirVisitor extends QueryPlanVisitor<InsertIntoDir> {
  private final SQLContext sqlContext;

  public InsertIntoDirVisitor(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
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
