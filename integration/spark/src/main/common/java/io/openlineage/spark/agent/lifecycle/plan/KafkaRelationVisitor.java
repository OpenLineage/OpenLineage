package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.kafka010.KafkaRelation;

/**
 * {@link LogicalPlan} visitor that attempts to extract a {@link OpenLineage.Dataset} from a {@link
 * LogicalRelation}. {@link org.apache.spark.sql.kafka010.KafkaRelation} is used to extract topic
 * and bootstrap servers for Kafka
 */
@Slf4j
public class KafkaRelationVisitor extends QueryPlanVisitor<LogicalRelation, OpenLineage.Dataset> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof LogicalRelation
        && ((LogicalRelation) x).relation() instanceof KafkaRelation;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    KafkaRelation relation = (KafkaRelation) ((LogicalRelation) x).relation();
    String server = "";
    String topic = "";
    try {
      Field sourceOptionsField = relation.getClass().getDeclaredField("sourceOptions");
      sourceOptionsField.setAccessible(true);
      CaseInsensitiveMap sourceOptions = (CaseInsensitiveMap) sourceOptionsField.get(relation);
      String servers =
          (String) asJavaOptional(sourceOptions.get("kafka.bootstrap.servers")).orElse("");
      topic =
          Stream.of("subscribe", "subscribePattern", "assign")
              .map(it -> sourceOptions.get(it))
              .filter(it -> it.nonEmpty())
              .map(it -> it.get())
              .map(String.class::cast)
              .findFirst()
              .orElse("");

      server = servers.split(",")[0];
    } catch (Exception e) {
      log.error("Can't extract kafka server option", e);
    }
    String namespace = "kafka://" + server;
    return Collections.singletonList(
        PlanUtils.getDataset(
            topic, namespace, PlanUtils.datasetFacet(relation.schema(), namespace)));
  }
}
