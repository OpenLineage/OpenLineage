/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.DefaultDatasetFacet;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import java.util.Map;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.junit.jupiter.api.Test;

/** Tests for {@link ConfigFacetVisitor}. */
class ConfigFacetVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  ConfigFacetVisitor visitor = new ConfigFacetVisitor(context);
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  LineageDataset flinkDataset = mock(LineageDataset.class);
  LineageDatasetWithIdentifier dataset = mock(LineageDatasetWithIdentifier.class);

  @Test
  void testIsDefinedAt() {
    when(dataset.getFlinkDataset()).thenReturn(flinkDataset);
    when(flinkDataset.facets()).thenReturn(Map.of("facet", mock(LineageDatasetFacet.class)));

    assertThat(visitor.isDefinedAt(dataset)).isFalse();

    when(flinkDataset.facets()).thenReturn(Map.of("config", mock(DatasetConfigFacet.class)));

    assertThat(visitor.isDefinedAt(dataset)).isTrue();
  }

  @Test
  void testApplyWithConfigFacet() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(dataset.getFlinkDataset()).thenReturn(flinkDataset);
    DatasetFacetsBuilder facetsBuilder = openLineage.newDatasetFacetsBuilder();

    DatasetConfigFacet configFacet = mock(DatasetConfigFacet.class);
    when(configFacet.name()).thenReturn("config");
    when(configFacet.config()).thenReturn(Map.of("key1", "value1", "key2", "value2"));
    when(flinkDataset.facets()).thenReturn(Map.of("config", configFacet));

    visitor.apply(dataset, facetsBuilder);

    Map<String, DatasetFacet> additionalProps = facetsBuilder.build().getAdditionalProperties();
    DefaultDatasetFacet resultFacet = (DefaultDatasetFacet) additionalProps.get("config");

    assertThat(resultFacet).isNotNull();
    assertThat(resultFacet.getAdditionalProperties()).containsEntry("key1", "value1");
    assertThat(resultFacet.getAdditionalProperties()).containsEntry("key2", "value2");
  }
}
