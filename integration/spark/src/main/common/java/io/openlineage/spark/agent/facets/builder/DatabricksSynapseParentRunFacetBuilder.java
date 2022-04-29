package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
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
      String parentRun = (String) jobStart.properties().get("spark.openlineage.internal.parentRun");
      boolean hasParentRun = (parentRun != null);
      return hasParentRun;
    } else {
      return false;
    }
  }

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super ParentRunFacet> consumer) {
    String parentRunId = event.properties().getProperty("spark.openlineage.internal.parentRun");
    UUID parentRunUuid = UUID.fromString(parentRunId);
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    consumer.accept(
        "parent",
        openLineage
            .newParentRunFacetBuilder()
            .run(openLineage.newParentRunFacetRun(parentRunUuid))
            .job(openLineage.newParentRunFacetJob("testnamespace", "testName"))
            .build());
  }
}
