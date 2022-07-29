/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

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
  public String getName() {
    return "unity";
  }
}
