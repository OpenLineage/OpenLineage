/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.gvfs.GVFSUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHadoopFsRelationCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHadoopFsRelationVisitor
    extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, OpenLineage.OutputDataset> {

  public InsertIntoHadoopFsRelationVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) x;

    Optional<DatasetIdentifier> di = getDatasetIdentifier(command);
    if (!di.isPresent()) {
      return Collections.emptyList();
    }

    OpenLineage.OutputDataset outputDataset;
    if (SaveMode.Overwrite == command.mode()) {
      outputDataset =
          outputDataset()
              .getDataset(
                  di.get(),
                  command.query().schema(),
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    } else {
      outputDataset = outputDataset().getDataset(di.get(), command.query().schema());
    }

    if (!command.catalogTable().isDefined()) {
      URI location = command.outputPath().toUri();
      if (GVFSUtils.isGVFS(location)) {
        outputDataset =
            GVFSUtils.injectGVFSFacets(context.getOpenLineage(), outputDataset, location);
      }
    }

    return Collections.singletonList(outputDataset);
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

    if (command.catalogTable().isDefined()) {
      return Optional.of(
          PathUtils.fromCatalogTable(
              command.catalogTable().get(), context.getSparkSession().get(), command.outputPath()));
    }
    return Optional.of(PathUtils.fromPath(command.outputPath()));
  }
}
