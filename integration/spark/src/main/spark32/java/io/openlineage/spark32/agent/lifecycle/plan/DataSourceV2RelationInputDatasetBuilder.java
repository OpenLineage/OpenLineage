/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark32.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark32.agent.utils.PlanUtils3;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table}.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table).
 */
@Slf4j
public class DataSourceV2RelationInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<DataSourceV2Relation> {

  private final DatasetFactory<OpenLineage.InputDataset> factory;

  public DataSourceV2RelationInputDatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.InputDataset> factory) {
    super(context, true);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof DataSourceV2Relation;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(DataSourceV2Relation relation) {
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();

    DatasetVersionDatasetFacetUtils.includeDatasetVersion(context, datasetFacetsBuilder, relation);
    return PlanUtils3.fromDataSourceV2Relation(factory, context, relation, datasetFacetsBuilder);
  }
}
