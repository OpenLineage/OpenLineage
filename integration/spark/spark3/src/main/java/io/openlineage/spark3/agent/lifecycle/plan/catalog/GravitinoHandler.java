/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.spark.agent.util.GravitinoUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class GravitinoHandler implements CatalogHandler {

  private static final String gravitinoCatalogClassName =
      "org.apache.gravitino.spark.connector.catalog.BaseCatalog";
  private final OpenLineageContext context;
  private final GravitinoInfoProviderImpl provider;

  public GravitinoHandler(OpenLineageContext context) {
    this.context = context;
    this.provider = GravitinoInfoProviderImpl.getInstance();
  }

  @Override
  public boolean hasClasses() {
    try {
      GravitinoHandler.class.getClassLoader().loadClass(gravitinoCatalogClassName);
      return true;
    } catch (Exception e) {
      log.debug("The Gravitino catalog is not present");
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof BaseCatalog;
  }

  @SneakyThrows
  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    try {
      String metalakeName = getGravitinoMetalakeName();
      log.debug(
          "Resolving Gravitino dataset identifier for catalog={}, identifier={}, metalake={}",
          tableCatalog.name(),
          identifier,
          metalakeName);
      return GravitinoUtils.getGravitinoDatasetIdentifier(
          metalakeName, tableCatalog.name(), tableCatalog.defaultNamespace(), identifier);
    } catch (IllegalStateException e) {
      log.error("Failed to get Gravitino metalake configuration: {}", e.getMessage());
      throw e;
    }
  }

  @Override
  public String getName() {
    return "gravitino";
  }

  private String getGravitinoMetalakeName() {
    return provider.getMetalakeName();
  }
}
