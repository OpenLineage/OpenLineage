package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

public abstract class AbstractQueryPlanInputDatasetBuilder<P extends LogicalPlan>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, P, OpenLineage.InputDataset> {

  public AbstractQueryPlanInputDatasetBuilder(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    // FIXME: missing test for this

    return event instanceof SparkListenerJobStart
        || event instanceof SparkListenerSQLExecutionStart;
  }
}
