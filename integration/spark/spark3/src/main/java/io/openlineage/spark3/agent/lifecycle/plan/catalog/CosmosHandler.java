/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.dataset.Naming;
import io.openlineage.client.utils.DatasetIdentifier;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class CosmosHandler implements RelationHandler {

  @Override
  public boolean hasClasses() {
    String COSMOS_CATALOG_NAME = "com.azure.cosmos.spark.CosmosCatalog";

    try {
      CosmosHandler.class.getClassLoader().loadClass(COSMOS_CATALOG_NAME);
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    try {
      Thread.currentThread().getContextClassLoader().loadClass(COSMOS_CATALOG_NAME);
      return true;
    } catch (Exception e) {
      // swallow- we don't care

    }
    return false;
  }

  @Override
  public boolean isClass(DataSourceV2Relation relation) {
    return relation.table().name().contains("com.azure.cosmos.spark.items.");
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation) {

    Naming.AzureCosmosDB.AzureCosmosDBBuilder builder = Naming.AzureCosmosDB.builder();
    String relationName = relation.table().name().replace("com.azure.cosmos.spark.items.", "");
    int expectedParts = 3;
    String[] tableParts = relationName.split("\\.", expectedParts);

    if (tableParts.length != expectedParts) {
      builder.table(relationName);
    } else {
      builder.host(tableParts[0]);
      builder.database(tableParts[1]);
      builder.table(tableParts[2]);
    }
    return new DatasetIdentifier(builder.build());
  }

  @Override
  public String getName() {
    return "cosmos";
  }
}
