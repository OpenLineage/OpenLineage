package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    Map<String, OpenLineage.DefaultDatasetFacet> facets =
        cmd.overwrite()
            ? Collections.singletonMap(
                "tableStateChange", new TableStateChangeFacet(StateChange.OVERWRITE))
            : Collections.emptyMap();

    return optionalUri
        .map(
            uri ->
                Collections.singletonList(
                    outputDataset()
                        .getDataset(PathUtils.fromURI(uri, "file"), cmd.child().schema(), facets)))
        .orElse(Collections.emptyList());
  }
}
