/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Lets a vendor rename a dataset. For example, Spark sees a Fabric lakehouse table as a folder of
 * files, and the Fabric vendor renames it to the table's name.
 *
 * <p>A vendor returns resolvers from {@link
 * OpenLineageEventHandlerFactory#createDatasetIdentifierResolvers}. {@link
 * #resolve(OpenLineageContext, DatasetIdentifier)} applies them to every dataset and every column
 * lineage input, so both get the same name.
 */
@FunctionalInterface
public interface DatasetIdentifierResolver {

  /**
   * @param identifier the dataset as Spark sees it, with its symlinks
   * @return the dataset to report, or {@code identifier} itself to keep it unchanged
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
