package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

@Slf4j
public class DataSourceV2ScanRelationInputBuilder
    extends AbstractQueryPlanInputDatasetBuilder<DataSourceV2ScanRelation> {

  private final DatasetFactory<OpenLineage.InputDataset> factory;

  public DataSourceV2ScanRelationInputBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.InputDataset> factory) {
    super(context);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof DataSourceV2ScanRelation;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(DataSourceV2ScanRelation scanRelation) {
    DataSourceV2Relation relation = (scanRelation).relation();
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();

    PlanUtils3.includeDatasetVersion(context, datasetFacetsBuilder, relation);
    return PlanUtils3.fromDataSourceV2Relation(factory, context, relation, datasetFacetsBuilder);
  }
}
