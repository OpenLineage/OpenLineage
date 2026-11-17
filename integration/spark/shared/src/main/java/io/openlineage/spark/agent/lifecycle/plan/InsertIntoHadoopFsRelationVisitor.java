/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.FileWriteTargetResolver;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark.api.SparkDatasetBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHadoopFsRelationCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 *
 * <p>Some connectors write files only as an intermediate step, for example to a temporary folder
 * that they then load into a table. A vendor's {@link FileWriteTargetResolver} can report that
 * table as the output instead of the folder. This must happen here: a separate visitor would add
 * the table as a second output and the folder would still be reported. The {@code outputStatistics}
 * facet, added by {@code OutputStatisticsOutputDatasetFacetBuilder}, still counts the intermediate
 * files.
 */
@Slf4j
public class InsertIntoHadoopFsRelationVisitor
    extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, OpenLineage.OutputDataset> {

  public InsertIntoHadoopFsRelationVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) x;
    Optional<FileWriteTargetResolver.Target> target = writeTarget(command);

    return getDatasetIdentifier(command, target)
        .map(
            di -> {
              SparkDatasetBuilder<OpenLineage.OutputDataset> builder =
                  outputDataset()
                      .sparkDatasetBuilder()
                      .dataset(di)
                      .schema(command.query().schema());
              // the save mode applies to the intermediate files, not to a vendor's output
              if (SaveMode.Overwrite == command.mode() && !target.isPresent()) {
                builder.lifecycleStateChange(
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
              }
              return Collections.singletonList(builder.build());
            })
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(InsertIntoHadoopFsRelationCommand command) {
    return getDatasetIdentifier(command, writeTarget(command))
        .map(di -> trimPath(context, di.getName()));
  }

  private Optional<FileWriteTargetResolver.Target> writeTarget(
      InsertIntoHadoopFsRelationCommand command) {
    for (FileWriteTargetResolver resolver : context.getFileWriteTargetResolvers()) {
      Optional<FileWriteTargetResolver.Target> target = resolve(resolver, command);
      if (target.isPresent()) {
        return target;
      }
    }
    return Optional.empty();
  }

  // If a vendor's resolver fails, the write is reported with its path, as if the vendor didn't
  // know it. LinkageError is caught too: vendor modules are compiled against one Spark version and
  // can meet another at runtime.
  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private static Optional<FileWriteTargetResolver.Target> resolve(
      FileWriteTargetResolver resolver, InsertIntoHadoopFsRelationCommand command) {
    try {
      return resolver.resolve(command);
    } catch (Exception | LinkageError e) {
      log.warn("File write resolver {} failed; reporting the write's path", resolver.getClass(), e);
      return Optional.empty();
    }
  }

  private Optional<DatasetIdentifier> getDatasetIdentifier(
      InsertIntoHadoopFsRelationCommand command, Optional<FileWriteTargetResolver.Target> target) {
    if (!context.getSparkSession().isPresent()) {
      return Optional.empty();
    }
    if (target.isPresent()) {
      return target.get().getDataset();
    }

    if (command.catalogTable().isDefined()) {
      return Optional.of(
          PathUtils.fromCatalogTable(
              command.catalogTable().get(), context.getSparkSession().get(), command.outputPath()));
    }
    return Optional.of(PathUtils.fromPath(command.outputPath()));
  }
}
