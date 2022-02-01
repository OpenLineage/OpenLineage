package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table} that implement the {@link DatasetSource} interface.
 */
@Slf4j
public class DataSourceV2ScanRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<DataSourceV2ScanRelation, D> {

  private final DatasetFactory<D> factory;

  public DataSourceV2ScanRelationVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  @Override
  public List<D> apply(LogicalPlan logicalPlan) {
    DataSourceV2ScanRelation dataSourceV2ScanRelation = (DataSourceV2ScanRelation) logicalPlan;
    return PlanUtils3.fromDataSourceV2Relation(
        factory, context, dataSourceV2ScanRelation.relation());
  }
}
