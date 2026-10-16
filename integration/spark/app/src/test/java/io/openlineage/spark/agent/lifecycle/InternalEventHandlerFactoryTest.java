/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory.NeverMatchingDatasetBuilder;
import io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory.TestRunFacetBuilder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;

class InternalEventHandlerFactoryTest {

  private static OpenLineageContext context;
  private static SparkContext sparkContext;

  @BeforeAll
  public static void setup() {
    sparkContext =
        SparkContext.getOrCreate(
            new SparkConf().setAppName("InternalEventHandlerFactoryTest").setMaster("local"));
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    context =
        OpenLineageContext.builder()
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();
  }

  @AfterAll
  public static void tearDown() {
    sparkContext.stop();
  }

  @Test
  void testHasTestRunFacet() {
    Collection<CustomFacetBuilder<?, ? extends RunFacet>> runFacetBuilders =
        new InternalEventHandlerFactory().createRunFacetBuilders(context);
    assertThat(runFacetBuilders)
        .isNotEmpty()
        .anyMatch(builder -> builder instanceof TestRunFacetBuilder);
  }

  @Test
  void testVendorDatasetBuildersPrecedeBuiltInOnes() {
    // vendor builders, e.g. for Snowflake relations, rely on resolving their nodes before the
    // generic LogicalRelationDatasetBuilder does
    InternalEventHandlerFactory factory = new InternalEventHandlerFactory();
    assertVendorBuilderPrecedesLogicalRelationBuilder(factory.createInputDatasetBuilder(context));
    assertVendorBuilderPrecedesLogicalRelationBuilder(factory.createOutputDatasetBuilder(context));
  }

  private static void assertVendorBuilderPrecedesLogicalRelationBuilder(
      Collection<? extends PartialFunction<Object, ?>> builders) {
    List<Class<?>> classes = new ArrayList<>();
    builders.forEach(builder -> classes.add(builder.getClass()));
    int vendor = classes.indexOf(NeverMatchingDatasetBuilder.class);
    int logicalRelation =
        classes.indexOf(
            io.openlineage.spark3.agent.lifecycle.plan.LogicalRelationDatasetBuilder.class);
    assertThat(vendor).isNotNegative().isLessThan(logicalRelation);
  }
}
