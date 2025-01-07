/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;

/** Interface for methods allowing translating Flink lineage facets into OpenLineage facets. */
public interface DatasetFacetVisitor {

  /**
   * Determines if visitor can be applied for a given Flink facet
   *
   * @param dataset
   * @return
   */
  boolean isDefinedAt(LineageDatasetWithIdentifier dataset);

  void apply(LineageDatasetWithIdentifier dataset, OpenLineage.DatasetFacetsBuilder builder);
}
