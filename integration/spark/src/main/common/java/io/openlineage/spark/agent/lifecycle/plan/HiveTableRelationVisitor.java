package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Simple visitor to support {@link HiveTableRelation}s in the plan. Both input and output {@link
 * Dataset}s are supported.
 *
 * @param <D>
 */
public class HiveTableRelationVisitor<D extends Dataset>
    extends QueryPlanVisitor<HiveTableRelation, D> {

  private final DatasetFactory<D> factory;

  public HiveTableRelationVisitor(@NonNull OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    HiveTableRelation hiveTable = (HiveTableRelation) x;
    DatasetIdentifier datasetId = PathUtils.fromCatalogTable(hiveTable.tableMeta());
    return Collections.singletonList(factory.getDataset(datasetId, x.schema()));
  }
}
