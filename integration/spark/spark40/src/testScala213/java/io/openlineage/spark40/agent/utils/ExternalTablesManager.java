/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import lombok.AllArgsConstructor;

/**
 * Computes external-table storage locations ({@code <catalog>/<schema>/<table>}) over a backend.
 */
@AllArgsConstructor
public final class ExternalTablesManager {

  StorageBackend backend;

  /** {@code <root>/<catalog>/<schema>/<table>}; does not create anything. */
  public String getLocation(String catalog, String schema, String table) {
    validate(catalog, "catalog");
    validate(schema, "schema");
    validate(table, "table");
    return backend.location(catalog + "/" + schema + "/" + table);
  }

  private static void validate(String value, String name) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " must not be null or empty");
    }
  }
}
