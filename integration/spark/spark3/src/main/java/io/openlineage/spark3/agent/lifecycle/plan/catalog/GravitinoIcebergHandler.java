/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.spark.agent.util.GravitinoUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class GravitinoIcebergHandler extends IcebergHandler {

  private GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.getInstance();

  public GravitinoIcebergHandler(OpenLineageContext context) {
    super(context);
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String metalake = provider.getMetalakeName();
    String catalogName = provider.getGravitinoCatalog(tableCatalog.name());
    return GravitinoUtils.getGravitinoDatasetIdentifier(
        metalake, catalogName, tableCatalog.defaultNamespace(), identifier);
  }
}
