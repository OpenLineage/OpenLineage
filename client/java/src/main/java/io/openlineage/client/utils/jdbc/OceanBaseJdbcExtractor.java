/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Implementation of {@link JdbcExtractor} for OceanBase.
 *
 * @see <a
 *     href="https://en.oceanbase.com/docs/common-oceanbase-connector-j-en-10000000000381036">OceanBase
 *     URL Format</a>
 */
public class OceanBaseJdbcExtractor implements JdbcExtractor {
  private static final String PROTOCOL_PART = "^[\\w+:]+://";

  private JdbcExtractor delegate() {
    return new OverridingJdbcExtractor("oceanbase", "2881");
  }

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return delegate().isDefinedAt(jdbcUri);
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    // Schema could be 'oceanbase', 'oceanbase:hamode'. Convert it to 'oceanbase'
    String normalizedUri = rawUri.replaceFirst(PROTOCOL_PART, "oceanbase://");
    return delegate().extract(normalizedUri, properties);
  }
}
