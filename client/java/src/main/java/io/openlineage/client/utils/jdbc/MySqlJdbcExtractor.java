/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

public class MySqlJdbcExtractor implements JdbcExtractor {
  // https://dev.mysql.com/doc/connector-j/en/connector-j-reference-jdbc-url-format.html

  private static final String PROTOCOL_PART = "^[\\w+:]+://";

  private JdbcExtractor delegate() {
    return new OverridingJdbcExtractor("mysql", "3306");
  }

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return delegate().isDefinedAt(jdbcUri);
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    // Schema could be 'mysql', 'mysql:part', 'mysql+srv:part'. Convert it to 'mysql'
    String normalizedUri = rawUri.replaceFirst(PROTOCOL_PART, "mysql://");
    return delegate().extract(normalizedUri, properties);
  }
}
