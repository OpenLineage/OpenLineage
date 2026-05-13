/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

/**
 * Extracts {@link DatasetConfigFacet} from any dataset. This handles both Table API and DataStream
 * API connectors that provide config facets via Flink's lineage API.
 */
public class ConfigFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  public ConfigFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    return dataset.getFlinkDataset().facets().values().stream()
        .anyMatch(f -> f instanceof DatasetConfigFacet);
  }

  @Override
  public void apply(LineageDatasetWithIdentifier dataset, DatasetFacetsBuilder builder) {
    for (LineageDatasetFacet facet : dataset.getFlinkDataset().facets().values()) {
      if (facet instanceof DatasetConfigFacet) {
        OpenLineage.DefaultDatasetFacet datasetFacet =
            (OpenLineage.DefaultDatasetFacet) context.getOpenLineage().newDatasetFacet();
        datasetFacet.getAdditionalProperties().putAll(((DatasetConfigFacet) facet).config());
        builder.put(facet.name(), datasetFacet);
      } else if (facet instanceof OpenLineage.DatasetFacet) {
        builder.put(facet.name(), (OpenLineage.DatasetFacet) facet);
      }
    }
  }
}
