package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches an {@link OptimizedCreateHiveTableAsSelectCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class OptimizedCreateHiveTableAsSelectCommandVisitor
    extends QueryPlanVisitor<OptimizedCreateHiveTableAsSelectCommand, OpenLineage.Dataset> {

  // OptimizedCreateHiveTableAsSelectCommand has been added in Spark 2.4.8
  public static boolean hasClasses() {
    try {
      BigQueryNodeVisitor.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return (plan instanceof OptimizedCreateHiveTableAsSelectCommand);
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    OptimizedCreateHiveTableAsSelectCommand command = (OptimizedCreateHiveTableAsSelectCommand) x;
    CatalogTable table = command.tableDesc();
    Path path = PlanUtils.getPath(table.location(), table.qualifiedName(), "");
    StructType schema = outputSchema(ScalaConversionUtils.fromSeq(command.outputColumns()));

    return Collections.singletonList(PlanUtils.getDataset(path.toUri(), schema));
  }

  private StructType outputSchema(List<Attribute> attrs) {
    return new StructType(
        attrs.stream()
            .map(a -> new StructField(a.name(), a.dataType(), a.nullable(), a.metadata()))
            .toArray(StructField[]::new));
  }
}
