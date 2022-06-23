/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory.TestRunFacetBuilder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collection;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
