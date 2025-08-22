/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Implementation of {@link JdbcExtractor} for DB2.
 *
 * @see <a href="https://www.ibm.com/docs/en/db2woc?topic=programmatically-jdbc">DB2 URL Format</a>
 */
public class Db2JdbcExtractor implements JdbcExtractor {

  private JdbcExtractor delegate() {
    return new OverridingJdbcExtractor("db2", "6789");
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
