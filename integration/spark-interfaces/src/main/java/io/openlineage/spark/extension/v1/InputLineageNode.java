/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.v1;

import java.util.List;

/**
 * Interface to be implemented for LogicalPlan nodes to extract lineage information about input
 * datasets.
 */
public interface InputLineageNode {
  /**
   * Gets input dataset read by this LogicalPlans node
   *
   * @param context OL extension context
   * @return list of input datasets with facets
   */
  List<InputDatasetWithFacets> getInputs(OpenLineageExtensionContext context);
}
