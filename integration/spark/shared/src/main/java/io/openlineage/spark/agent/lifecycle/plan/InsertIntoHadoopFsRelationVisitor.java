/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.FabricWarehouseSparkUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark.api.SparkDatasetBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHadoopFsRelationCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 *
 * <p>Microsoft's Spark connector for Fabric warehouses writes a table in two steps, and this
 * command is the first one: saving Parquet files to a temporary folder. For those writes, the
 * output is the warehouse table, not the temporary folder; see {@link FabricWarehouseSparkUtils}.
 *
 * <p>Such an output has no {@code lifecycleStateChange}. Its {@code outputStatistics}, added
 * separately by {@code OutputStatisticsOutputDatasetFacetBuilder}, describe the first step: the row
 * count matches the rows loaded into the table, but the byte size and file count are those of the
 * temporary files.
 */
public class InsertIntoHadoopFsRelationVisitor
    extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, OpenLineage.OutputDataset> {

  public InsertIntoHadoopFsRelationVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) x;
    boolean fabricWarehouseWrite = isFabricWarehouseWrite(command);

    return getDatasetIdentifier(command)
        .map(
            di -> {
              SparkDatasetBuilder<OpenLineage.OutputDataset> builder =
                  outputDataset()
                      .sparkDatasetBuilder()
                      .dataset(di)
                      .schema(command.query().schema());
              // The Fabric connector's first step always overwrites its temporary folder, whatever
              // mode the user chose (append, overwrite, errorifexists). The user's mode isn't
              // visible anywhere, so no lifecycle change is reported rather than a wrong one.
              if (SaveMode.Overwrite == command.mode() && !fabricWarehouseWrite) {
                builder.lifecycleStateChange(
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
              }
              return Collections.singletonList(builder.build());
            })
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(InsertIntoHadoopFsRelationCommand command) {
    return getDatasetIdentifier(command).map(di -> trimPath(context, di.getName()));
  }

  private Optional<DatasetIdentifier> getDatasetIdentifier(
      InsertIntoHadoopFsRelationCommand command) {
    if (!context.getSparkSession().isPresent()) {
      return Optional.empty();
    }

    if (isFabricWarehouseWrite(command)) {
      // This command only writes the Fabric connector's temporary folder. Report the warehouse
      // table it loads instead, or no output if the table's address can't be worked out. This
      // has to happen in this visitor: a separate one would add a second output next to the
      // temporary folder instead of replacing it.
      return FabricWarehouseSparkUtils.getWriteIdentifier(
          options(command), FabricWarehouseSparkUtils.confOf(context.getSparkSession().get()));
    }

    if (command.catalogTable().isDefined()) {
      return Optional.of(
          PathUtils.fromCatalogTable(
              command.catalogTable().get(), context.getSparkSession().get(), command.outputPath()));
    }
    return Optional.of(PathUtils.fromPath(command.outputPath()));
  }

  private static boolean isFabricWarehouseWrite(InsertIntoHadoopFsRelationCommand command) {
    return FabricWarehouseSparkUtils.isConnectorWrite(options(command));
  }

  private static Map<String, String> options(InsertIntoHadoopFsRelationCommand command) {
    if (command.options() == null) {
      return Collections.emptyMap();
    }
    return ScalaConversionUtils.fromMap(command.options());
  }
}
