package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.DatasetSource;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table} that implement the {@link DatasetSource} interface.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table).
 */
public class DatasetSourceVisitor extends QueryPlanVisitor<LogicalPlan> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return findDatasetSource(x).isPresent();
  }

  private Optional<DatasetSource> findDatasetSource(LogicalPlan plan) {
    if (plan instanceof LogicalRelation) {
      if (((LogicalRelation) plan).relation() instanceof DatasetSource) {
        return Optional.of((DatasetSource) ((LogicalRelation) plan).relation());
      }
    } else if (plan instanceof DataSourceV2Relation) {
      DataSourceV2Relation relation = (DataSourceV2Relation) plan;
      if (relation.table() instanceof DatasetSource) {
        return Optional.of((DatasetSource) relation.table());
      }
    }

    return Optional.empty();
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    DatasetSource datasetSource =
        findDatasetSource(x)
            .orElseThrow(() -> new RuntimeException("Couldn't find DatasetSource in plan " + x));
    return Collections.singletonList(
        PlanUtils.getDataset(
            datasetSource.name(),
            datasetSource.namespace(),
            PlanUtils.datasetFacet(x.schema(), datasetSource.namespace())));
  }
}
