package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.SparkContext;
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

  public static boolean hasHiveClasses() {
    try {
      InsertIntoHiveTableVisitor.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.hive.execution.InsertIntoHiveTable");
      return true;
    } catch (ClassNotFoundException e) {
      // ignore
    }
    return false;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    InsertIntoHiveTable cmd = (InsertIntoHiveTable) x;
    CatalogTable table = cmd.table();

    String authority =
        SparkConfUtils.findSparkConfigKey(context.getConf(), metastoreUriKey)
            .orElse(
                SparkConfUtils.findSparkConfigKey(context.getConf(), metastoreHadoopUriKey)
                    .orElse(""));

    return Collections.singletonList(PlanUtils.getDataset(table, authority));
  }
}
