/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class TableCatalogStorage {
  private static final Map<String, StoredTableCatalog> catalogs = new HashMap<>();

  public static void put(String key, StoredTableCatalog catalog) {
    catalogs.put(key, catalog);
  }

  public static Optional<StoredTableCatalog> get(String key) {
    return Optional.ofNullable(catalogs.get(key));
  }

  @Getter
  @RequiredArgsConstructor(staticName = "of")
  public static class StoredTableCatalog {
    private final TableCatalog tableCatalog;
    private final Map<String, String> tableProperties;
  }
}
