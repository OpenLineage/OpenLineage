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
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class GravitinoJDBCHandler extends JdbcHandler {

  private GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.getInstance();

  public GravitinoJDBCHandler(OpenLineageContext context) {
    super(context);
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String originalCatalogName = tableCatalog.name();
    String metalake = provider.getMetalakeName();
    String catalogName = provider.getGravitinoCatalog(originalCatalogName);

    if (!originalCatalogName.equals(catalogName)) {
      log.debug(
          "JDBC catalog name mapped: {} -> {} for identifier {}",
          originalCatalogName,
          catalogName,
          identifier);
    } else {
      log.debug(
          "Resolving JDBC dataset identifier for catalog={}, identifier={}, metalake={}",
          catalogName,
          identifier,
          metalake);
    }

    return GravitinoUtils.getGravitinoDatasetIdentifier(
        metalake, catalogName, tableCatalog.defaultNamespace(), identifier);
  }
}
