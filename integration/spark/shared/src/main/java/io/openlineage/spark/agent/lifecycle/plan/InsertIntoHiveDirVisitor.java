/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHiveDirCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHiveDirVisitor
    extends QueryPlanVisitor<InsertIntoHiveDirCommand, OpenLineage.OutputDataset> {

  public InsertIntoHiveDirVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHiveDirCommand cmd = (InsertIntoHiveDirCommand) x;
    Optional<URI> optionalUri = ScalaConversionUtils.asJavaOptional(cmd.storage().locationUri());

    return optionalUri
        .map(
            uri -> {
              OpenLineage.OutputDataset outputDataset;
              if (cmd.overwrite()) {
                outputDataset =
                    outputDataset()
                        .getDataset(
                            PathUtils.fromURI(uri, "file"),
                            cmd.query().schema(),
                            OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                                .OVERWRITE);
              } else {
                outputDataset =
                    outputDataset()
                        .getDataset(PathUtils.fromURI(uri, "file"), cmd.query().schema());
              }
              return Collections.singletonList(outputDataset);
            })
        .orElse(Collections.emptyList());
  }
}
