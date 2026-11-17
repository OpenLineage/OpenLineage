/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URISyntaxException;
import java.util.Properties;

public interface JdbcExtractor {
  public abstract boolean isDefinedAt(String jdbcUri);

  public abstract JdbcLocation extract(String rawUri, Properties properties)
      throws URISyntaxException;

  /**
   * Adds details from the connection properties to a dataset, such as a symlink to the location of
   * its files. Called after the dataset is named from {@link #extract}.
   *
   * @param identifier the dataset, named from {@code location}
   * @param location the location {@link #extract} returned
   * @param properties the connection properties, which can include a connector's own options
   * @return the dataset, by default unchanged
   */
  default DatasetIdentifier enrich(
      DatasetIdentifier identifier, JdbcLocation location, Properties properties) {
    return identifier;
  }
}
