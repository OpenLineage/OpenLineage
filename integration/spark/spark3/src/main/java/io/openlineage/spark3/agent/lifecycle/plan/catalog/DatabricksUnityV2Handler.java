/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/**
 * The DatabricksUnityV2Handler is intended to support Databricks' custom Unity Catalog which has
 * the class name of com.databricks.sql.managedcatalog.UnityCatalogV2Proxy rather than the open
 * source class name of org.apache.spark.sql.delta.catalog.DeltaCatalog. It is used in the same way
 * as the {@link DeltaHandler}.
 */
@Slf4j
public class DatabricksUnityV2Handler extends AbstractDatabricksHandler {

  public DatabricksUnityV2Handler(OpenLineageContext context) {
    super(context, "com.databricks.sql.managedcatalog.UnityCatalogV2Proxy");
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    return Optional.of(
        context
            .getOpenLineage()
            .newStorageDatasetFacet("unity", "parquet")); // The default is parquet / delta
  }

  @Override
  public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog tableCatalog, Map<String, String> properties) {
    OpenLineage.CatalogDatasetFacetBuilder builder =
        context
            .getOpenLineage()
            .newCatalogDatasetFacetBuilder()
            .name(tableCatalog.name())
            .framework("delta")
            .type("unity")
            .source("spark");

    return Optional.of(CatalogWithAdditionalFacets.of(builder.build()));
  }

  @Override
  public String getName() {
    return "unity";
  }
}
