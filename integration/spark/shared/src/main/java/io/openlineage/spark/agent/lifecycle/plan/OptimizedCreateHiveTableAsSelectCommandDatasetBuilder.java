/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Dataset builder that matches an {@link OptimizedCreateHiveTableAsSelectCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class OptimizedCreateHiveTableAsSelectCommandDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<OptimizedCreateHiveTableAsSelectCommand> {

  public OptimizedCreateHiveTableAsSelectCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  // OptimizedCreateHiveTableAsSelectCommand has been added in Spark 2.4.8
  public static boolean hasClasses() {
    try {
      OptimizedCreateHiveTableAsSelectCommandDatasetBuilder.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand");
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If class does not exist or it's loading fails for some reason, we handle that failure by
      // returning false
    }
    return false;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan plan) {
    return (plan instanceof OptimizedCreateHiveTableAsSelectCommand);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(OptimizedCreateHiveTableAsSelectCommand command) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    CatalogTable table = command.tableDesc();
    DatasetIdentifier datasetIdentifier =
        PathUtils.fromCatalogTable(table, context.getSparkSession().get());
    StructType schema = outputSchema(ScalaConversionUtils.fromSeq(command.outputColumns()));

    SparkDatasetBuilder<OpenLineage.OutputDataset> builder =
        outputDataset().sparkDatasetBuilder().dataset(datasetIdentifier).schema(schema);
    if ((SaveMode.Overwrite == command.mode())) {
      builder.lifecycleStateChange(
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    }

    return Collections.singletonList(builder.build());
  }

  private StructType outputSchema(List<Attribute> attrs) {
    return new StructType(
        attrs.stream()
            .map(a -> new StructField(a.name(), a.dataType(), a.nullable(), a.metadata()))
            .toArray(StructField[]::new));
  }
}
