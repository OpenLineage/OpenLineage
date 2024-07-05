/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage;
import java.util.List;

/**
 * Interface to be implemented for LogicalPlan nodes to extract lineage information about output
 * datasets.
 */
public interface OutputLineageNode {
  /**
   * Gets output dataset read by this LogicalPlans node
   *
   * @param sparkListenerEventName class name of SparkListenerEvent that triggered
   *     OpenLineageSparkListener
   * @param openLineage instance to create OpenLineage objects
   * @return list of dataset with facets
   */
  List<OutputDatasetWithFacets> getOutputs(String sparkListenerEventName, OpenLineage openLineage);
}
