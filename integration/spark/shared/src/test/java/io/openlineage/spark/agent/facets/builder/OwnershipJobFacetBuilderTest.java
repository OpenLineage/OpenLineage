/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OwnershipJobFacet;
import io.openlineage.client.OpenLineageYaml;
import io.openlineage.client.config.JobConfig;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class OwnershipJobFacetBuilderTest {

  OpenLineageContext olContext = mock(OpenLineageContext.class);
  OwnershipJobFacetBuilder builder = new OwnershipJobFacetBuilder(olContext);

  @Test
  void testIsDefined() {
    assertThat(builder.isDefinedAt(mock(SparkListenerEvent.class))).isTrue();
  }

  @Test
  void testBuildWhenNoFacetConfig() {
    OpenLineageYaml openLineageYaml = mock(OpenLineageYaml.class);
    JobConfig jobConfig = mock(JobConfig.class);
    BiConsumer<String, ? super OwnershipJobFacet> consumer = mock(BiConsumer.class);

    when(olContext.getOpenLineageYaml()).thenReturn(openLineageYaml);
    when(openLineageYaml.getJobConfig()).thenReturn(jobConfig);
    when(jobConfig.getJobOwners()).thenReturn(null);

    builder.build(mock(SparkListenerEvent.class), consumer);

    verify(consumer, Mockito.times(0)).accept(any(), any());
  }

  @Test
  void testBuild() {
    OpenLineageYaml openLineageYaml = mock(OpenLineageYaml.class);
    JobConfig jobConfig = mock(JobConfig.class);
    BiConsumer<String, ? super OwnershipJobFacet> consumer = mock(BiConsumer.class);

    Map<String, String> conf = new HashMap<>();
    conf.put("team", "MyTeam");
    conf.put("person", "John Smith");

    when(olContext.getOpenLineageYaml()).thenReturn(openLineageYaml);
    when(olContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(openLineageYaml.getJobConfig()).thenReturn(jobConfig);
    when(jobConfig.getJobOwners()).thenReturn(conf);

    builder.build(mock(SparkListenerEvent.class), consumer);
    verify(consumer)
        .accept(
            eq("ownership"),
            argThat(
                (OwnershipJobFacet facet) ->
                    facet.getOwners().size() == 2
                        && facet.getOwners().stream()
                            .filter(o -> o.getName().equals("MyTeam") && o.getType().equals("team"))
                            .findAny()
                            .isPresent()
                        && facet.getOwners().stream()
                            .filter(
                                o ->
                                    o.getName().equals("John Smith")
                                        && o.getType().equals("person"))
                            .findAny()
                            .isPresent()));
  }
}
