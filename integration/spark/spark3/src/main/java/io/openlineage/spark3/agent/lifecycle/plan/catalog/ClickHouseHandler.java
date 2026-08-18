/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/**
 * The ClickHouseHandler supports the official ClickHouse Spark connector
 * (com.clickhouse:clickhouse-spark), whose catalog class is com.clickhouse.spark.ClickHouseCatalog.
 * The connector is detected by class name, so there is no compile-time dependency on it.
 *
 * <p>Dataset identifiers use the same convention as {@code JdbcDatasetUtils} for {@code
 * jdbc:clickhouse://} URLs - a {@code clickhouse://host:port} namespace with a database-qualified
 * table name - so a table reached through this catalog and the same table reached through Spark
 * JDBC produce the same dataset.
 */
public class ClickHouseHandler implements CatalogHandler {
  private static final String CATALOG_CLASS_NAME = "com.clickhouse.spark.ClickHouseCatalog";
  private static final String CLICKHOUSE = "clickhouse";
  private static final String CATALOG_CONF_PREFIX = "spark.sql.catalog.";
  private static final String PROTOCOL_NATIVE = "native";
  private static final String PROTOCOL_TCP = "tcp";
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_HTTP_PORT = "8123";
  private static final String DEFAULT_TCP_PORT = "9000";
  private static final String ENGINE_PROPERTY = "engine";
  private static final String UNKNOWN_ENGINE = "unknown";

  private final OpenLineageContext context;

  public ClickHouseHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    try {
      ClickHouseHandler.class.getClassLoader().loadClass(CATALOG_CLASS_NAME);
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If the class does not exist or loading it fails, this handler is not available.
    }

    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    if (contextClassLoader != null) {
      try {
        contextClassLoader.loadClass(CATALOG_CLASS_NAME);
        return true;
      } catch (NoClassDefFoundError | Exception e) {
        // If the class does not exist or loading it fails, this handler is not available.
      }
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return CATALOG_CLASS_NAME.equals(tableCatalog.getClass().getName());
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    RuntimeConfig conf = session.conf();
    String prefix = CATALOG_CONF_PREFIX + catalog.name();
    String host = conf.get(prefix + ".host", DEFAULT_HOST);
    String port = resolvePort(conf, prefix);
    String name =
        Stream.concat(Arrays.stream(identifier.namespace()), Stream.of(identifier.name()))
            .collect(Collectors.joining("."));

    return new DatasetIdentifier(name, CLICKHOUSE + "://" + host + ":" + port);
  }

  /**
   * The connector serves queries over http ({@code http_port}, 8123 by default) or the native tcp
   * protocol ({@code tcp_port}, 9000 by default), selected by the {@code protocol} option. Any
   * other value - including the connector's http default - falls back to the http port.
   */
  private String resolvePort(RuntimeConfig conf, String prefix) {
    String protocol = conf.get(prefix + ".protocol", "http");
    if (PROTOCOL_NATIVE.equalsIgnoreCase(protocol) || PROTOCOL_TCP.equalsIgnoreCase(protocol)) {
      return conf.get(prefix + ".tcp_port", DEFAULT_TCP_PORT);
    }
    return conf.get(prefix + ".http_port", DEFAULT_HTTP_PORT);
  }

  @Override
  public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog catalog, Map<String, String> properties) {
    OpenLineage.CatalogDatasetFacetBuilder builder =
        context
            .getOpenLineage()
            .newCatalogDatasetFacetBuilder()
            .name(catalog.name())
            .framework(CLICKHOUSE)
            .type(CLICKHOUSE)
            .source("spark");

    return Optional.of(CatalogWithAdditionalFacets.of(builder.build()));
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    // ClickHouseTable.properties() exposes the table spec, including the table engine
    // (MergeTree, ReplacingMergeTree, ...); fall back when no engine is known, e.g. when
    // extracting from a CREATE TABLE command.
    return Optional.of(
        context
            .getOpenLineage()
            .newStorageDatasetFacet(
                CLICKHOUSE, properties.getOrDefault(ENGINE_PROPERTY, UNKNOWN_ENGINE)));
  }

  @Override
  public String getName() {
    return CLICKHOUSE;
  }
}
