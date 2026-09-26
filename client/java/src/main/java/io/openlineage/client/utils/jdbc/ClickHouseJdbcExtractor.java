/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of {@link JdbcExtractor} for ClickHouse.
 *
 * @see <a href="https://clickhouse.com/docs/en/integrations/java#jdbc-driver">ClickHouse JDBC
 *     Driver</a>
 */
public class ClickHouseJdbcExtractor implements JdbcExtractor {

  private static final int MIN_QUALIFIED_TABLE_NAME_PARTS = 2;

  private JdbcExtractor delegate() {
    return new OverridingJdbcExtractor("clickhouse", "8123");
  }

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    // Support both 'ch' and 'clickhouse' schemes
    return jdbcUri.startsWith("ch:") || jdbcUri.startsWith("clickhouse:");
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    // Normalize scheme and remove protocol prefix (similar to how PostgreSQL removes SSL info)
    String normalizedUri =
        rawUri
            .replaceFirst("^ch:", "clickhouse:")
            .replaceFirst("^clickhouse:(https?|grpc)://", "clickhouse://");

    JdbcLocation location = delegate().extract(normalizedUri, properties);

    // In ClickHouse, DATABASE and SCHEMA are synonyms (no 3-level structure)
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
