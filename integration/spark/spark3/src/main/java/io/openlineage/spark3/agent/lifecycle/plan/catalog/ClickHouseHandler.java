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

public class ClickHouseHandler implements CatalogHandler {
  private static final String CATALOG_CLASS_NAME = "com.clickhouse.spark.ClickHouseCatalog";
  private static final String CLICKHOUSE = "clickhouse";

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
    String prefix = "spark.sql.catalog." + catalog.name();
    String host = conf.get(prefix + ".host", "localhost");
    String port = conf.get(prefix + ".http_port", "8123");
    String name =
        Stream.concat(Arrays.stream(identifier.namespace()), Stream.of(identifier.name()))
            .collect(Collectors.joining("."));

    return new DatasetIdentifier(name, "clickhouse://" + host + ":" + port);
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
  public String getName() {
    return CLICKHOUSE;
  }
}
