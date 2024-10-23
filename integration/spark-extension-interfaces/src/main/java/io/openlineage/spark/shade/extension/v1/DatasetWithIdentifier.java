/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Represents a dataset with an identifier that includes the dataset's namespace and name.
 *
 * <p>Implementing classes should provide a method to retrieve the {@link DatasetIdentifier}, which
 * encapsulates the dataset's unique namespace and name. This identifier follows the naming
 * conventions outlined in the <a href="https://openlineage.io/docs/spec/naming/">OpenLineage Naming
 * Specification</a>, ensuring consistency across datasets for lineage tracking and data cataloging.
 */
interface DatasetWithIdentifier {
  /**
   * Returns the {@link DatasetIdentifier}, which contains the namespace and name of the dataset.
   *
   * @return the dataset identifier containing the namespace and name
   */
  DatasetIdentifier getDatasetIdentifier();
}
