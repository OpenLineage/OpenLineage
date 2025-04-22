/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier.catalog;

import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import java.util.Optional;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.listener.CatalogContext;

public class GenericInMemoryCatalogSymlinkProvider implements CatalogSymlinkProvider {

  private static final String NAMESPACE = "flink://";

  @Override
  public Optional<Symlink> getSymlink(CatalogContext catalogContext, LineageDataset dataset) {
    String tableName = dataset.name();
    tableName = removeCatalogName(catalogContext.getCatalogName(), tableName);

    return Optional.of(new Symlink(tableName, NAMESPACE, SymlinkType.TABLE));
  }

  @Override
  public boolean isDefinedAt(Class<? extends Catalog> catalogClazz) {
    return GenericInMemoryCatalog.class.isAssignableFrom(catalogClazz);
  }

  private String removeCatalogName(String catalogName, String tableName) {
    String catalogPrefix = catalogName + ".";
    if (tableName.startsWith(catalogPrefix)) {
      // remove catalog name from table name
      return tableName.replaceFirst(catalogPrefix, ""); // should be replace first
    }
    return tableName;
  }
}
