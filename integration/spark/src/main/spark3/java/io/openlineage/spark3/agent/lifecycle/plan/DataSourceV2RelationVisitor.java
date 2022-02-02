/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table} that implement the {@link DatasetSource} interface.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table).
 */
@Slf4j
public class DataSourceV2RelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<DataSourceV2Relation, D> {

  private final DatasetFactory<D> factory;
  private final boolean isInputVisitor;

  public DataSourceV2RelationVisitor(
      OpenLineageContext context, DatasetFactory<D> factory, boolean isInputVisitor) {
    super(context);
    this.factory = factory;
    this.isInputVisitor = isInputVisitor;
  }

  @Override
  public List<D> apply(LogicalPlan logicalPlan) {
    return PlanUtils3.fromDataSourceV2Relation(
        factory, context, (DataSourceV2Relation) logicalPlan);
  }
}
