/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.Getter;
import org.apache.spark.scheduler.SparkListenerJobEnd;

/**
 * Test implementation - writes a custom {@link TestRunFacet} for every {@link SparkListenerJobEnd}
 * event.
 */
public class TestOpenLineageEventHandlerFactory implements OpenLineageEventHandlerFactory {

  public static final String TEST_FACET_KEY = "test_event_handler_factory_run_facet";

  @Override
  public List<CustomFacetBuilder<?, ? extends RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return Collections.singletonList(new TestRunFacetBuilder());
  }

  @Getter
  public static class TestRunFacet extends OpenLineage.DefaultRunFacet {
    private final String message;

    public TestRunFacet(String message) {
      super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
      this.message = message;
    }
  }

  public static class TestRunFacetBuilder
      extends CustomFacetBuilder<SparkListenerJobEnd, TestRunFacet> {

    @Override
    protected void build(
        SparkListenerJobEnd event, BiConsumer<String, ? super TestRunFacet> consumer) {
      consumer.accept(TEST_FACET_KEY, new TestRunFacet(String.valueOf(event.jobId())));
    }
  }
}
