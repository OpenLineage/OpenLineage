/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

/**
 * Represents a dataset associated with a node in a Spark LogicalPlan.
 *
 * <p>Implementing classes should provide a method to retrieve the node from which an input or
 * output dataset can be extracted. This is used to capture lineage information by identifying the
 * node in the Spark LogicalPlan that corresponds to the dataset.
 */
interface DatasetWithDelegate {
  /**
   * Returns the node in the LogicalPlan from which the dataset is extracted.
   *
   * @return the node object representing the location in the LogicalPlan
   */
  Object getNode();
}
