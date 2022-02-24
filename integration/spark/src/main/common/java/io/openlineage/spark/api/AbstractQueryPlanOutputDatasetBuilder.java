package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

public abstract class AbstractQueryPlanOutputDatasetBuilder<P extends LogicalPlan>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, P, OpenLineage.OutputDataset> {

  public AbstractQueryPlanOutputDatasetBuilder(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    // FIXME: missing test for this
    return event instanceof SparkListenerJobEnd || event instanceof SparkListenerSQLExecutionEnd;
  }
}
