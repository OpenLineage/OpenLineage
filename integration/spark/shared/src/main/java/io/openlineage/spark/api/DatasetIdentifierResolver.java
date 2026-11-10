/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Gives a dataset its final name, for example a platform's logical name for a table that Spark
 * reached through its files. Vendors contribute resolvers through {@link
 * OpenLineageEventHandlerFactory#createDatasetIdentifierResolvers}, and {@link
 * #resolve(OpenLineageContext, DatasetIdentifier)} applies them to every dataset and every column
 * lineage input, so both are named the same way.
 */
@FunctionalInterface
public interface DatasetIdentifierResolver {

  /**
   * @param identifier the dataset as Spark reached it, with its symlinks
   * @return the identifier to report, or {@code identifier} itself to leave it unchanged
   */
  DatasetIdentifier resolve(DatasetIdentifier identifier);

  /**
   * Applies the context's resolvers, in order.
   *
   * @return the dataset's final identifier, or {@code identifier} itself if no resolver changed it
   */
  static DatasetIdentifier resolve(OpenLineageContext context, DatasetIdentifier identifier) {
    DatasetIdentifier resolved = identifier;
    for (DatasetIdentifierResolver resolver : context.getDatasetIdentifierResolvers()) {
      resolved = resolver.resolve(resolved);
    }
    return resolved;
  }
}
