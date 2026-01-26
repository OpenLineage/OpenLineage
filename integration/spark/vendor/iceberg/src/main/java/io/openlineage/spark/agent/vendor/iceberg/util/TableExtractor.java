/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.util;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.net.URI;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.spark.sql.catalyst.TableIdentifier;

public class TableExtractor {
  public Optional<TableIdentifier> extractTableIdentifier(Table table) {
    String name = table.name();
    if (name == null || name.trim().isEmpty()) {
      return Optional.empty();
    }

    String[] parts = name.split("\\.");
    if (parts.length == 0) {
      return Optional.empty();
    }

    String tableName = parts[parts.length - 1];
    String databaseName = parts.length > 1 ? parts[parts.length - 2] : null;
    return Optional.of(
        new TableIdentifier(tableName, ScalaConversionUtils.toScalaOption(databaseName)));
  }

  public Optional<URI> extractTableLocation(Table table) {
    String location = table.location();
    if (location == null || location.trim().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new Path(location).toUri());
  }
}
