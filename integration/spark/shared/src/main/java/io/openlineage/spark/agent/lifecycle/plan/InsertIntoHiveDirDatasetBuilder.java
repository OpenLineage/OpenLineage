/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand;

/**
 * Dataset builder that matches an {@link InsertIntoHiveDirCommand} and extracts the output {@link
 * OpenLineage.Dataset} being written.
 */
public class InsertIntoHiveDirDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<InsertIntoHiveDirCommand> {

  public InsertIntoHiveDirDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof InsertIntoHiveDirCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(InsertIntoHiveDirCommand cmd) {
    Optional<URI> optionalUri = ScalaConversionUtils.asJavaOptional(cmd.storage().locationUri());

    return optionalUri
        .map(
            uri -> {
              SparkDatasetBuilder<OpenLineage.OutputDataset> builder =
                  outputDataset()
                      .sparkDatasetBuilder()
                      .dataset(uri)
                      .schema(cmd.query().schema())
                      .catalog();
              if (cmd.overwrite()) {
                builder.lifecycleStateChange(LifecycleStateChange.OVERWRITE);
              }
              return Collections.singletonList(builder.build());
            })
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(InsertIntoHiveDirCommand command) {
    return ScalaConversionUtils.asJavaOptional(command.storage().locationUri())
        .map(uri -> trimPath(context, PathUtils.fromURI(uri).getName()));
  }
}
