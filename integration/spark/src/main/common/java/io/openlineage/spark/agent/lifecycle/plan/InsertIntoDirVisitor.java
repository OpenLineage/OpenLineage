/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
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
              DatasetIdentifier di = PathUtils.fromURI(uri, "file");
              OpenLineage.OutputDataset outputDataset;
              if (cmd.overwrite()) {
                outputDataset =
                    outputDataset()
                        .getDataset(
                            di,
                            cmd.child().schema(),
                            OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                                .OVERWRITE);
              } else {
                outputDataset = outputDataset().getDataset(di, cmd.child().schema());
              }
              return Collections.singletonList(outputDataset);
            })
        .orElse(Collections.emptyList());
  }
}
