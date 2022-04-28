package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * {@link CustomFacetBuilder} that generates a {@link ParentRunFacet} when using OpenLineage on
 * Databricks.
 */
@Slf4j
public class DatabricksSynapseParentRunFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobStart, ParentRunFacet> {
  private final OpenLineageContext openLineageContext;

  public DatabricksSynapseParentRunFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    if (x instanceof SparkListenerJobStart) {
      SparkListenerJobStart jobStart = (SparkListenerJobStart) x;
      return jobStart.properties().contains("openlineage.databricks.parentRun");
    } else {
      return false;
    }
  }

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super ParentRunFacet> consumer) {
    SparkListenerJobStart jobStart = (SparkListenerJobStart) event;
    String parentRunId = event.properties().getProperty("openlineage.databricks.parentRun");
    Optional<UUID> parentRunUuid = convertToUUID(parentRunId);
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    consumer.accept(
        "parent",
        openLineage
            .newParentRunFacetBuilder()
            .run(openLineage.newParentRunFacetRun(parentRunUuid.get()))
            .job(openLineage.newParentRunFacetJob("testnamespace", "testName"))
            .build());
  }

  private static Optional<UUID> convertToUUID(String uuid) {
    try {
      return Optional.ofNullable(uuid).map(UUID::fromString);
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
