package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

/**
 * Class extending {@link io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor} with
 * methods only available for Spark3. It is required to support datasetVersionFacet for delta
 * provider
 */
@Slf4j
public class LogicalRelationVisitor
    extends io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor<OpenLineage.Dataset> {

  public LogicalRelationVisitor(
      OpenLineageContext context, DatasetFactory datasetFactory, boolean isInputVisitor) {
    super(context, datasetFactory, isInputVisitor);
  }

  @Override
  protected void includeDatasetVersionFacet(
      Map<String, OpenLineage.DatasetFacet> facets, LogicalRelation x) {
    DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(x)
        .ifPresent(
            version ->
                facets.put(
                    "datasetVersion",
                    context.getOpenLineage().newDatasetVersionDatasetFacet(version)));
  }
}
