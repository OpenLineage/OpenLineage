/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

public class MySqlJdbcExtractor implements JdbcExtractor {
  // https://dev.mysql.com/doc/connector-j/en/connector-j-reference-jdbc-url-format.html

  private static final String PROTOCOL_PART = "^[\\w+:]+://";
  private static final int MIN_QUALIFIED_TABLE_NAME_PARTS = 2;

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

    JdbcLocation location = delegate().extract(normalizedUri, properties);

    // In MySQL, DATABASE and SCHEMA are synonyms (no 3-level structure)
    // Override toName() to handle qualified vs unqualified table names
    return new JdbcLocation(
        location.getScheme(),
        location.getAuthority(),
        location.getInstance(),
        location.getDatabase()) {
      @Override
      public String toName(List<String> parts) {
        if (parts.size() >= MIN_QUALIFIED_TABLE_NAME_PARTS) {
          return String.join(".", parts);
        }
        // Otherwise, use default behavior (add URL database)
        return super.toName(parts);
      }
    };
  }
}
