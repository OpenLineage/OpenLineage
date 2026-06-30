/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Implementation of {@link JdbcExtractor} for Cassandra.
 *
 * @see <a
 *     href="https://github.com/ing-bank/cassandra-jdbc-wrapper/wiki/JDBC-driver-and-connection-string">Cassandra
 *     JDBC Driver</a>
 */
public class CassandraJdbcExtractor implements JdbcExtractor {

  private JdbcExtractor delegate() {
    return new OverridingJdbcExtractor("cassandra", "9042");
  }

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return delegate().isDefinedAt(jdbcUri);
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    String normalizedUri = rawUri.replaceAll("--", ",");

    return delegate().extract(normalizedUri, properties);
  }
}
