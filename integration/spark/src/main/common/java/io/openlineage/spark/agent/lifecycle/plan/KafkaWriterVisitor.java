package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.kafka010.KafkaSourceProvider;

/**
 * {@link LogicalPlan} visitor that matches an {@link KafkaSourceProvider} and extracts the output
 * {@link OpenLineage.Dataset} being written. Gets the first host from 'kafka.bootstrap.servers'
 * option and creates namespace Extracts a 'topic' option and put it as dataset name
 *
 * <p>Write to Kafka using a topic specified in the data is not supported.
 */
public class KafkaWriterVisitor
    extends QueryPlanVisitor<SaveIntoDataSourceCommand, OpenLineage.Dataset> {

  public static boolean hasKafkaClasses() {
    try {
      KafkaWriterVisitor.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.kafka010.KafkaSourceProvider");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof SaveIntoDataSourceCommand
        && hasKafkaClasses()
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
