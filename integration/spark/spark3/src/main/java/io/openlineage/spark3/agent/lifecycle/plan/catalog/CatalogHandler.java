/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public interface CatalogHandler {

  @Getter
  class CatalogWithAdditionalFacets {

    private final OpenLineage.CatalogDatasetFacet catalogDatasetFacet;
    private final Map<String, OpenLineage.DatasetFacet> additionalFacets = new HashMap<>();

    private CatalogWithAdditionalFacets(OpenLineage.CatalogDatasetFacet catalogDatasetFacet) {
      this.catalogDatasetFacet = catalogDatasetFacet;
    }

    public void addFacet(String key, OpenLineage.DatasetFacet facet) {
      additionalFacets.put(key, facet);
    }

    public static CatalogWithAdditionalFacets of(
        OpenLineage.CatalogDatasetFacet catalogDatasetFacet) {
      return new CatalogWithAdditionalFacets(catalogDatasetFacet);
    }
  }

  boolean hasClasses();

  boolean isClass(TableCatalog tableCatalog);

  DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties);

  default Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    return Optional.empty();
  }

  default Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog tableCatalog, Map<String, String> properties) {
    return Optional.empty();
  }

  /** Try to find string that uniquely identifies version of a dataset. */
  default Optional<String> getDatasetVersion(
      TableCatalog catalog, Identifier identifier, Map<String, String> properties) {
    return Optional.empty();
  }

  String getName();
}
