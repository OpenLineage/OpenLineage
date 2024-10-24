/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import org.apache.flink.streaming.api.lineage.LineageDataset;

/** Interface for methods allowing translating Flink lineage facets into OpenLineage facets. */
public interface DatasetFacetVisitor {

  /**
   * Determines if visitor can be applied for a given Flink facet
   *
   * @param flinkDataset
   * @return
   */
  boolean isDefinedAt(LineageDataset flinkDataset);

  void apply(LineageDataset flinkDataset, OpenLineage.DatasetFacetsBuilder builder);
}
