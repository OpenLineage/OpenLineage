/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier.catalog;

import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import java.util.Optional;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.listener.CatalogContext;

/**
 * Interface for providing symlinks for tables in a catalog.
 *
 * <p>This interface is used to retrieve the symlink information for a given table in a catalog.
 * Implementations of this interface should provide the logic to extract the symlink information
 * based on the specific catalog type.
 */
public interface CatalogSymlinkProvider {

  /**
   * Returns a {@link Symlink} for the given table if it is defined.
   *
   * @param catalogContext the catalog context
   * @param dataset the dataset
   * @return an optional containing the symlink if defined, empty otherwise
   */
  Optional<Symlink> getSymlink(CatalogContext catalogContext, LineageDataset dataset);

  /**
   * Checks if the provider is defined for the given catalog class.
   *
   * @param catalogClazz the catalog class
   * @return true if the provider is defined for the catalog class, false otherwise
   */
  boolean isDefinedAt(Class<? extends Catalog> catalogClazz);
}
