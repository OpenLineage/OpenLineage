/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;

@Slf4j
public class JdbcHandler implements CatalogHandler {
  private final OpenLineageContext context;
  private final DatasetNamespaceCombinedResolver namespaceResolver;

  public JdbcHandler(OpenLineageContext context) {
    this.context = context;
    namespaceResolver = new DatasetNamespaceCombinedResolver(context.getOpenLineageConfig());
  }

  @Override
  public boolean hasClasses() {
    return true;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof JDBCTableCatalog;
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    Optional<JDBCOptions> jdbcOptions = getJdbcOptions(tableCatalog);

    if (!jdbcOptions.isPresent()) {
      throw new MissingDatasetIdentifierCatalogException(
          "No jdbc options found for catalog " + tableCatalog.name());
    }

    JDBCOptions options = jdbcOptions.get();
    List<String> parts =
        Stream.concat(Arrays.stream(identifier.namespace()), Stream.of(identifier.name()))
            .collect(Collectors.toList());

    return namespaceResolver.resolve(
        JdbcDatasetUtils.getDatasetIdentifier(
            options.url(), parts, options.asConnectionProperties()));
  }

  @Override
  public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog tableCatalog, Map<String, String> properties) {
    Optional<JDBCOptions> options = getJdbcOptions(tableCatalog);

    OpenLineage.CatalogDatasetFacetBuilder builder =
        context
            .getOpenLineage()
            .newCatalogDatasetFacetBuilder()
            .name(tableCatalog.name())
            .framework("jdbc")
            .type("jdbc")
            .source("spark");

    options.ifPresent(jdbcOptions -> builder.metadataUri(options.get().url()));

    return Optional.of(CatalogWithAdditionalFacets.of(builder.build()));
  }

  private Optional<JDBCOptions> getJdbcOptions(TableCatalog tableCatalog) {
    try {
      JDBCTableCatalog catalog = (JDBCTableCatalog) tableCatalog;
      JDBCOptions jdbcOptions = (JDBCOptions) FieldUtils.readField(catalog, "options", true);
      return Optional.of(jdbcOptions);
    } catch (IllegalAccessException e) {
      log.warn("Failed to access JDBC Options", e);
      return Optional.empty();
    }
  }

  @Override
  public String getName() {
    return "jdbc";
  }
}
