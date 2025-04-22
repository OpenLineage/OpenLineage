/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Implementation of {@link JdbcExtractor} for CreateDB.
 *
 * @see <a href="https://cratedb.com/docs/jdbc/en/latest/connect.html">CrateDB URL Format</a>
 */
public class CrateJdbcExtractor implements JdbcExtractor {

  private JdbcExtractor delegate() {
    return new OverridingJdbcExtractor("crate", "5432");
  }

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return delegate().isDefinedAt(jdbcUri);
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    return delegate().extract(rawUri, properties);
  }
}
