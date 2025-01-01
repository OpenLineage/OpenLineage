/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Interface to be implemented by extension classes that extend {@code
 * org.apache.spark.sql.sources.BaseRelation}.
 *
 * <p>Implementing this interface allows for the extraction of lineage information from {@code
 * BaseRelation} objects. The methods {@code getNamespace} and {@code getName}, provided by {@link
 * DatasetIdentifier}, must be implemented by the classes that implement this interface. This
 * identifier must follow the naming conventions outlined in the <a
 * href="https://openlineage.io/docs/spec/naming/">OpenLineage Naming Specification</a>, ensuring
 * consistency across datasets for lineage tracking and data cataloging.
 */
public interface LineageRelation {
  /**
   * Returns a {@link DatasetIdentifier} containing the namespace and name of the dataset for
   * lineage tracking purposes.
   *
   * @param sparkListenerEventName the name of the Spark listener event triggering the lineage
   *     extraction
   * @param openLineage an instance of {@link OpenLineage} used for lineage-related operations
   * @return a {@link DatasetIdentifier} representing the dataset associated with the event
   */
  DatasetIdentifier getLineageDatasetIdentifier(
      String sparkListenerEventName, OpenLineage openLineage);
}
