/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark.api.SparkDatasetCompositeFacetsBuilder;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoDir;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDir} and extracts the output {@link
 * OpenLineage.Dataset} being written.
 */
public class InsertIntoDirVisitor
    extends QueryPlanVisitor<InsertIntoDir, OpenLineage.OutputDataset> {

  public InsertIntoDirVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoDir cmd = (InsertIntoDir) x;
    Optional<URI> optionalUri = ScalaConversionUtils.asJavaOptional(cmd.storage().locationUri());

    return optionalUri
        .map(
            uri -> {
              SparkDatasetCompositeFacetsBuilder<OpenLineage.OutputDataset> builder =
                  outputDataset().sparkDatasetBuilder().dataset(uri).schema(cmd.child().schema());
              if (cmd.overwrite()) {
                builder.lifecycleStateChange(
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
              }

              return Collections.singletonList(builder.build());
            })
        .orElse(Collections.emptyList());
  }
}
