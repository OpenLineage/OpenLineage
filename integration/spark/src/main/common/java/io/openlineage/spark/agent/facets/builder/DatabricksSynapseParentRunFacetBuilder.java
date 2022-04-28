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
    log.info("WILLJ: Synapse Parent Run is being checked");
    if (x instanceof SparkListenerJobStart) {
      SparkListenerJobStart jobStart = (SparkListenerJobStart) x;
      String parentRun = (String) jobStart.properties().get("openlineage.databricks.parentRun");
      boolean hasParentRun = (parentRun != null);
      log.info(String.format("WILLJ: HasParentRun== %s", Boolean.toString(hasParentRun)));
      return hasParentRun;
    } else {
      return false;
    }
  }

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super ParentRunFacet> consumer) {
    SparkListenerJobStart jobStart = (SparkListenerJobStart) event;
    log.info("WILLJ: Parent Run Facet Builder is Building!");
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
