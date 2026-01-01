/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

public class PostgresJdbcExtractor implements JdbcExtractor {
  // https://jdbc.postgresql.org/documentation/use/#connecting-to-the-database

  private JdbcExtractor delegate() {
    return new OverridingJdbcExtractor("postgres", "5432");
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
