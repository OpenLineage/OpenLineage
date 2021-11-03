package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.kafka010.KafkaSourceProvider;

public class KafkaWriterVisitor
    extends QueryPlanVisitor<SaveIntoDataSourceCommand, OpenLineage.Dataset> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof SaveIntoDataSourceCommand
        && ((SaveIntoDataSourceCommand) x).dataSource() instanceof KafkaSourceProvider;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    SaveIntoDataSourceCommand saveIntoDataSourceCommand = (SaveIntoDataSourceCommand) x;

    String servers =
        ScalaConversionUtils.asJavaOptional(
                saveIntoDataSourceCommand.options().get("kafka.bootstrap.servers"))
            .orElse("");
    String namespace = "kafka://" + servers.split(",")[0];
    String topic =
        ScalaConversionUtils.asJavaOptional(saveIntoDataSourceCommand.options().get("topic"))
            .orElse("");
    return Collections.singletonList(
        PlanUtils.getDataset(topic, namespace, PlanUtils.datasetFacet(x.schema(), namespace)));
  }
}
