/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;

/**
 * Handles Iceberg catalogs backed by a JDBC/relational-database catalog implementation (Iceberg's
 * {@code org.apache.iceberg.jdbc.JdbcCatalog}, configured with {@code type=jdbc} or {@code
 * catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog}).
 *
 * <p>Without this handler, {@link IcebergHandler} falls back to {@link HiveCatalogTypeHandler} for
 * unrecognized catalog types, which tries to parse the catalog's {@code uri} property (a JDBC
 * connection string, e.g. {@code jdbc:postgresql://host:5432/db}) as a Hive Thrift metastore URI.
 * Since a JDBC URI is opaque (has no URI authority), that produces an invalid {@code hive:} URI and
 * throws an uncaught {@link java.net.URISyntaxException} from deep inside dataset extraction -
 * silently dropping the dataset (input or output) from the emitted lineage event instead of just
 * failing to attach a symlink. See https://github.com/OpenLineage/OpenLineage/issues/4677.
 */
@Slf4j
class JdbcCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String JDBC_CATALOG_TYPE = "jdbc";
  private static final String JDBC_CATALOG_IMPL_SUFFIX = "JdbcCatalog";
  private static final String JDBC_PROPERTY_PREFIX = "jdbc.";

  @Override
  String getType() {
    return JDBC_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return JDBC_CATALOG_TYPE.equalsIgnoreCase(catalogConf.get(TYPE))
        || (catalogConf.containsKey(CATALOG_IMPL)
            && catalogConf.get(CATALOG_IMPL).endsWith(JDBC_CATALOG_IMPL_SUFFIX));
  }

  @Override
  Optional<DatasetIdentifier> getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    String jdbcUrl = catalogConf.get(CatalogProperties.URI);
    if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
      log.debug(
          "Iceberg JDBC catalog is missing the '{}' property; cannot build a symlink for {}",
          CatalogProperties.URI,
          table);
      return Optional.empty();
    }

    Properties jdbcProperties = new Properties();
    catalogConf.forEach(
        (key, value) -> {
          if (key.startsWith(JDBC_PROPERTY_PREFIX) && value != null) {
            jdbcProperties.setProperty(key.substring(JDBC_PROPERTY_PREFIX.length()), value);
          }
        });

    // Reuses the same JDBC URL -> DatasetIdentifier resolution used for plain JDBC datasets
    // elsewhere in the Spark integration, so an Iceberg JDBC-catalog table symlinks to the same
    // namespace a directly-queried JDBC table on that database would use.
    return Optional.of(JdbcDatasetUtils.getDatasetIdentifier(jdbcUrl, table, jdbcProperties));
  }
}
