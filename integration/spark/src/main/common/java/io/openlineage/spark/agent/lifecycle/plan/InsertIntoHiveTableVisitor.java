package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHiveTable} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHiveTableVisitor
    extends QueryPlanVisitor<InsertIntoHiveTable, OpenLineage.Dataset> {

  private final SparkContext context;

  private final String metastoreUriKey = "spark.sql.hive.metastore.uris";
  private final String metastoreHadoopUriKey = "spark.hadoop.hive.metastore.uris";

  public InsertIntoHiveTableVisitor(SparkContext context) {
    this.context = context;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    InsertIntoHiveTable cmd = (InsertIntoHiveTable) x;
    CatalogTable table = cmd.table();
    Path path;

    try {
      path = new Path(table.location());
      if (table.location().getScheme() == null) {
        path = new Path("file", null, table.location().toString());
      }
    } catch (Exception e) { // Java does not recognize scala exception
      if (e instanceof AnalysisException) {

        String authority =
            SparkConfUtils.findSparkConfigKey(context.getConf(), metastoreUriKey)
                .orElse(
                    SparkConfUtils.findSparkConfigKey(context.getConf(), metastoreHadoopUriKey)
                        .get());

        path = new Path("hive", authority, table.qualifiedName());
      }
      throw e;
    }
    return Collections.singletonList(PlanUtils.getDataset(path.toUri(), cmd.query().schema()));
  }
}
