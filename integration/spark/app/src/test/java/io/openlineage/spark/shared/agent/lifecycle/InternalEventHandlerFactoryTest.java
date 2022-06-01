/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.shared.agent.Versions;
import io.openlineage.spark.app.agent.util.TestOpenLineageEventHandlerFactory.TestRunFacetBuilder;
import io.openlineage.spark.shared.agent.lifecycle.InternalEventHandlerFactory;
import io.openlineage.spark.shared.api.CustomFacetBuilder;
import io.openlineage.spark.shared.api.OpenLineageContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class InternalEventHandlerFactoryTest {

  private static OpenLineageContext context;
  private static SparkContext sparkContext;

  @BeforeAll
  public static void setup() {
    sparkContext =
        SparkContext.getOrCreate(
            new SparkConf().setAppName("InternalEventHandlerFactoryTest").setMaster("local"));
    context =
        OpenLineageContext.builder()
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .build();
  }

  @AfterAll
  public static void tearDown() {
    sparkContext.stop();
  }

  @Test
  public void testHasTestRunFacet() {
    Collection<CustomFacetBuilder<?, ? extends RunFacet>> runFacetBuilders =
        new InternalEventHandlerFactory().createRunFacetBuilders(context);
    assertThat(runFacetBuilders)
        .isNotEmpty()
        .anyMatch(builder -> builder instanceof TestRunFacetBuilder);
  }
}
