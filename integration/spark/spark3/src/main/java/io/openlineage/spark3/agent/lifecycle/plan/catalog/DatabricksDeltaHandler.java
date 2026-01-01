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
 * The DatabricksDeltaHandler is intended to support Databricks' custom DeltaCatalog which has the
 * class name of com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog rather than the open
 * source class name of org.apache.spark.sql.delta.catalog.DeltaCatalog. It is used in the same way
 * as the {@link DeltaHandler}.
 */
@Slf4j
public class DatabricksDeltaHandler extends AbstractDatabricksHandler {
  private static final String DELTA = "delta";

  public DatabricksDeltaHandler(OpenLineageContext context) {
    super(context, "com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog");
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    return Optional.of(
        context
            .getOpenLineage()
            .newStorageDatasetFacet(DELTA, "parquet")); // Delta is always parquet
  }

  @Override
  public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog tableCatalog, Map<String, String> properties) {
    OpenLineage.CatalogDatasetFacetBuilder builder =
        context
            .getOpenLineage()
            .newCatalogDatasetFacetBuilder()
            .name(tableCatalog.name())
            .framework(DELTA)
            .type(DELTA)
            .source("spark");

    return Optional.of(CatalogWithAdditionalFacets.of(builder.build()));
  }

  @Override
  public String getName() {
    return DELTA;
  }
}
