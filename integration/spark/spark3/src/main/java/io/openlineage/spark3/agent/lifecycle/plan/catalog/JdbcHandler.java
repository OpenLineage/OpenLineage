/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.JdbcUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;

public class JdbcHandler implements CatalogHandler {
  private final DatasetNamespaceCombinedResolver namespaceResolver;

  public JdbcHandler(OpenLineageContext context) {
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

  @SneakyThrows
  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    JDBCTableCatalog catalog = (JDBCTableCatalog) tableCatalog;
    JDBCOptions options = (JDBCOptions) FieldUtils.readField(catalog, "options", true);

    List<String> parts =
        Stream.concat(Arrays.stream(identifier.namespace()), Stream.of(identifier.name()))
            .collect(Collectors.toList());

    return namespaceResolver.resolve(
        JdbcUtils.getDatasetIdentifierFromJdbcUrl(options.url(), parts));
  }

  @Override
  public String getName() {
    return "jdbc";
  }
}
