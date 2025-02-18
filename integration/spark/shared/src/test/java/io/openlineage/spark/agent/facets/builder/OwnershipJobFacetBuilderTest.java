/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
import io.openlineage.client.job.JobConfig;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class OwnershipJobFacetBuilderTest {

  OpenLineageContext olContext = mock(OpenLineageContext.class);
  OwnershipJobFacetBuilder builder = new OwnershipJobFacetBuilder(olContext);

  @Test
  void testIsDefined() {
    assertThat(builder.isDefinedAt(mock(SparkListenerEvent.class))).isTrue();
  }

  @Test
  void testBuildWhenNoFacetConfig() {
    SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
    JobConfig jobConfig = mock(JobConfig.class);

    BiConsumer<String, ? super OwnershipJobFacet> consumer = mock(BiConsumer.class);

    when(olContext.getOpenLineageConfig()).thenReturn(config);
    when(config.getJobConfig()).thenReturn(jobConfig);
    when(jobConfig.getOwners()).thenReturn(null);

    builder.build(mock(SparkListenerEvent.class), consumer);

    verify(consumer, Mockito.times(0)).accept(any(), any());
  }

  @Test
  void testBuild() {
    Map<String, String> owners = new HashMap<>();
    owners.put("team", "MyTeam");
    owners.put("person", "John Smith");

    SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
    JobConfig jobConfig = mock(JobConfig.class);
    JobConfig.JobOwnersConfig jobOwnersConfig = mock(JobConfig.JobOwnersConfig.class);

    BiConsumer<String, ? super OwnershipJobFacet> consumer = mock(BiConsumer.class);

    when(olContext.getOpenLineageConfig()).thenReturn(config);
    when(config.getJobConfig()).thenReturn(jobConfig);
    when(jobConfig.getOwners()).thenReturn(jobOwnersConfig);
    when(jobOwnersConfig.getAdditionalProperties()).thenReturn(owners);

    when(olContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));

    builder.build(mock(SparkListenerEvent.class), consumer);
    verify(consumer)
        .accept(
            eq("ownership"),
            argThat(
                (OwnershipJobFacet facet) ->
                    facet.getOwners().size() == 2
                        && facet.getOwners().stream()
                            .filter(o -> "MyTeam".equals(o.getName()) && "team".equals(o.getType()))
                            .findAny()
                            .isPresent()
                        && facet.getOwners().stream()
                            .filter(
                                o ->
                                    "John Smith".equals(o.getName())
                                        && "person".equals(o.getType()))
                            .findAny()
                            .isPresent()));
  }
}
