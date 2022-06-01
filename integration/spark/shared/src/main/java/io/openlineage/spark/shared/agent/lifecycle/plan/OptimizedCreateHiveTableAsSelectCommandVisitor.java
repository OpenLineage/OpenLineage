/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
import io.openlineage.spark.shared.agent.util.PathUtils;
import io.openlineage.spark.shared.agent.util.ScalaConversionUtils;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark.shared.api.QueryPlanVisitor;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

/**
 * {@link LogicalPlan} visitor that matches an {@link OptimizedCreateHiveTableAsSelectCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class OptimizedCreateHiveTableAsSelectCommandVisitor
    extends QueryPlanVisitor<OptimizedCreateHiveTableAsSelectCommand, OpenLineage.OutputDataset> {

  public OptimizedCreateHiveTableAsSelectCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  // OptimizedCreateHiveTableAsSelectCommand has been added in Spark 2.4.8
  public static boolean hasClasses() {
    try {
      OptimizedCreateHiveTableAsSelectCommandVisitor.class
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
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    OptimizedCreateHiveTableAsSelectCommand command = (OptimizedCreateHiveTableAsSelectCommand) x;
    CatalogTable table = command.tableDesc();
    DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(table);
    StructType schema = outputSchema(ScalaConversionUtils.fromSeq(command.outputColumns()));

    OpenLineage.OutputDataset outputDataset;
    if ((SaveMode.Overwrite == command.mode())) {
      outputDataset =
          outputDataset()
              .getDataset(
                  datasetIdentifier,
                  schema,
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    } else {
      outputDataset = outputDataset().getDataset(datasetIdentifier, schema);
    }

    return Collections.singletonList(outputDataset);
  }

  private StructType outputSchema(List<Attribute> attrs) {
    return new StructType(
        attrs.stream()
            .map(a -> new StructField(a.name(), a.dataType(), a.nullable(), a.metadata()))
            .toArray(StructField[]::new));
  }
}
