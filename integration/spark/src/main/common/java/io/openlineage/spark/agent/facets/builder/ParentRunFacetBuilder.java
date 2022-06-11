package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.AbstractRunFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.NonNull;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;

public class ParentRunFacetBuilder extends AbstractRunFacetBuilder<SparkListenerEvent> {
  private static final Map<Long, UUID> executionIdToRunIdMap = new ConcurrentHashMap<>();

  public ParentRunFacetBuilder(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  protected void build(SparkListenerEvent event, BiConsumer<String, ? super RunFacet> consumer) {
    context
        .getSparkSession()
        .ifPresent(
            sess -> {
              Optional<ParentRunFacet> parentFacet = buildParentFacet(sess);
              parentFacet.ifPresent(facet -> consumer.accept("parent", facet));
            });
  }

  private Optional<ParentRunFacet> buildParentFacet(SparkSession sess) {
    // This property appears to be unique to Azure Databricks' implementation
    // of a connector to Azure Synapse SQL Pools
    // String sqlExecutionParent =
    //     SparkConfUtils.findSparkConfigKey(sess.sparkContext().conf(), "sql.execution.parent",
    // null);

    // // Better to do an IF and then call method or call method with IF inside it?
    // String parentRunId = executionParentToParentRun(sqlExecutionParent);
    String parentRunId = null;
    if (parentRunId == null) {
      parentRunId =
          SparkConfUtils.findSparkConfigKey(
              sess.sparkContext().conf(), "openlineage.parentRunId", null);
    }
    String parentJobName =
        SparkConfUtils.findSparkConfigKey(
            sess.sparkContext().conf(), "openlineage.parentJobName", null);
    String namespace =
        SparkConfUtils.findSparkConfigKey(
            sess.sparkContext().conf(), "openlineage.namespace", null);
    if (parentRunId != null && parentJobName != null && namespace != null) {
      return Optional.of(
          context
              .getOpenLineage()
              .newParentRunFacetBuilder()
              .run(
                  context
                      .getOpenLineage()
                      .newParentRunFacetRunBuilder()
                      .runId(UUID.fromString(parentRunId))
                      .build())
              .job(
                  context
                      .getOpenLineage()
                      .newParentRunFacetJobBuilder()
                      .namespace(namespace)
                      .name(parentJobName)
                      .build())
              .build());
    }
    return Optional.empty();
  }

  private String executionParentToParentRun(String sqlExecutionParent) {
    String output = null;
    // Test to see if the sqlExecutionParent was present, if so extract the stored runid
    if (sqlExecutionParent != null) {
      if (executionIdToRunIdMap.containsKey(Long.parseLong(sqlExecutionParent))) {
        output = executionIdToRunIdMap.get(Long.parseLong(sqlExecutionParent)).toString();
      } else {
        output = null;
      }
    }
    return output;
  }
}
