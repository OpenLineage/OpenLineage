/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricWarehouseUtils;
import java.net.URISyntaxException;
import java.util.Locale;
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

  /**
   * Adds the location of the table's files in OneLake, when the read comes from Microsoft's Spark
   * connector for Fabric and the user set the warehouse ID ({@code DatawarehouseId}); see {@link
   * FabricWarehouseUtils#withOneLakeLocation}.
   *
   * <p>The ID is for the warehouse the query runs in: the connector's {@code database_name} option,
   * or else the database in the URL. A query can also read from other warehouses in the same
   * workspace. Their tables don't get a location, because their IDs aren't known.
   */
  @Override
  public DatasetIdentifier enrich(
      DatasetIdentifier identifier, JdbcLocation location, Properties properties) {
    Optional<String> queryWarehouse =
        property(properties, FabricWarehouseUtils.DATABASE_NAME_OPTION);
    return FabricWarehouseUtils.withOneLakeLocation(
        identifier,
        queryWarehouse.isPresent() ? queryWarehouse : location.getDatabase(),
        property(properties, FabricWarehouseUtils.WORKSPACE_ID_OPTION),
        property(properties, FabricWarehouseUtils.DATAWAREHOUSE_ID_OPTION));
  }

  // Spark keeps the option names as the user wrote them, so match them in any case
  private static Optional<String> property(Properties properties, String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return properties.stringPropertyNames().stream()
        .filter(name -> name.toLowerCase(Locale.ROOT).equals(lower))
        .map(properties::getProperty)
        // a blank option is the same as no option
        .filter(value -> !value.trim().isEmpty())
        .findFirst();
  }
}
