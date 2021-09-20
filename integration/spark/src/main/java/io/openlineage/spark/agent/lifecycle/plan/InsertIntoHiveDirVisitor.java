package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHiveDirCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHiveDirVisitor extends QueryPlanVisitor<InsertIntoHiveDirCommand> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    InsertIntoHiveDirCommand cmd = (InsertIntoHiveDirCommand) x;
    CatalogStorageFormat storage = cmd.storage();
    return ScalaConversionUtils.asJavaOptional(storage.locationUri())
        .map(
            uri -> {
              Path path = new Path(uri);
              if (uri.getScheme() == null) {
                path = new Path("file", null, uri.toString());
              }
              return Collections.singletonList(
                  PlanUtils.getDataset(path.toUri(), cmd.query().schema()));
            })
        .orElse(Collections.emptyList());
  }
}
