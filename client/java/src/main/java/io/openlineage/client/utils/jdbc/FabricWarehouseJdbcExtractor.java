/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import io.openlineage.client.utils.FabricWarehouseUtils;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;

/**
 * Fabric warehouses are reached through ordinary SQL Server connection URLs. This names tables read
 * through such a URL as {@code fabric-warehouse://<address>} instead of {@code
 * sqlserver://<address>}, with the address normalized.
 *
 * <p>The URL is read the same way as any SQL Server URL, including the {@code
 * sqlserver://;servername=<host>;...} form that Microsoft's Spark connector for Fabric uses.
 *
 * <p>Only the URL itself is checked. If the address is passed only as a connection property, the
 * table keeps the {@code sqlserver} name.
 */
public class FabricWarehouseJdbcExtractor implements JdbcExtractor {
  private final SqlServerJdbcExtractor delegate = new SqlServerJdbcExtractor();

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    if (!delegate.isDefinedAt(jdbcUri)) {
      return false;
    }
    try {
      return delegate
          .extract(jdbcUri, new Properties())
          .getAuthority()
          .map(FabricWarehouseUtils::isFabricEndpoint)
          .orElse(false);
    } catch (URISyntaxException e) {
      return false;
    }
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    JdbcLocation location = delegate.extract(rawUri, properties);
    return new JdbcLocation(
        FabricWarehouseUtils.SCHEME,
        location.getAuthority().map(FabricWarehouseUtils::canonicalEndpoint),
        Optional.empty(),
        location.getDatabase());
  }
}
