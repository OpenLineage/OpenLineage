package io.openlineage.spark2.agent.lifecycle.plan;

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
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link DataSourceV2} readers and
 * writers that implement the {@link DatasetSource} interface.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table). While the {@link DataSourceV2Relation}
 * is a {@link LogicalPlan}, its {@link DataSourceReader} and {@link
 * org.apache.spark.sql.sources.v2.writer.DataSourceWriter} fields are not. Thus, the only (current)
 * way of extracting the actual dataset name is to attempt to cast the {@link DataSourceReader}
 * and/or {@link org.apache.spark.sql.sources.v2.writer.DataSourceWriter} instances to {@link
 * DatasetSource}s.
 */
public class DatasetSourceVisitor extends QueryPlanVisitor<LogicalPlan, OpenLineage.Dataset> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return findDatasetSource(x).isPresent();
  }

  private Optional<DatasetSource> findDatasetSource(LogicalPlan plan) {
    if (plan instanceof LogicalRelation) {
      if (((LogicalRelation) plan).relation() instanceof DatasetSource) {
        return Optional.of((DatasetSource) ((LogicalRelation) plan).relation());
      }
      // Check the DataSourceV2Relation's reader.
      // Note that we don't check the writer here as it is always encapsulated by the
      // WriteToDataSourceV2 LogicalPlan below.
    } else if (plan instanceof DataSourceV2Relation) {
      DataSourceV2Relation relation = (DataSourceV2Relation) plan;
      DataSourceV2 source = relation.source();
      DataSourceV2Relation dataSourceV2Relation =
          DataSourceV2Relation.create(
              source, relation.options(), relation.tableIdent(), relation.userSpecifiedSchema());
      DataSourceReader reader = dataSourceV2Relation.newReader();
      if (reader instanceof DatasetSource) {
        return Optional.of((DatasetSource) dataSourceV2Relation);
      }

      // Check the WriteToDataSourceV2's writer
    } else if (plan instanceof WriteToDataSourceV2
        && ((WriteToDataSourceV2) plan).writer() instanceof DatasetSource) {
      return Optional.of((DatasetSource) ((WriteToDataSourceV2) plan).writer());
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
