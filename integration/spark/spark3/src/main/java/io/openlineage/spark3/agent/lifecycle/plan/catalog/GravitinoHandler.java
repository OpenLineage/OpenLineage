/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfo;
import io.openlineage.client.utils.gravitino.GravitinoInfoManager;
import io.openlineage.spark.agent.util.GravitinoUtils;
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
  private final GravitinoInfoManager provider;

  public GravitinoHandler() {
    this.provider = GravitinoInfoManager.getInstance();
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
      GravitinoInfo gravitinoInfo = provider.getGravitinoInfo();
      log.debug(
          "Resolving Gravitino dataset identifier for catalog={}, identifier={}, metalake={}",
          tableCatalog.name(),
          identifier,
          gravitinoInfo.getMetalake().orElse("unknown"));
      return GravitinoUtils.getGravitinoDatasetIdentifier(
          gravitinoInfo, tableCatalog.name(), tableCatalog.defaultNamespace(), identifier);
    } catch (IllegalStateException e) {
      log.error("Failed to get Gravitino configuration: {}", e.getMessage());
      throw e;
    }
  }

  @Override
  public String getName() {
    return "gravitino";
  }
}
