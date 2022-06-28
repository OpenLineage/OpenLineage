/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CatalogUtils3Test {

  @Test
  void testGetCatalogHandlerByProviderForIceberg() {
    assertTrue(
        CatalogUtils3.getCatalogHandlerByProvider("Iceberg").get() instanceof IcebergHandler);
  }

  @Test
  void testGetCatalogHandlerByProviderForDelta() {
    assertTrue(CatalogUtils3.getCatalogHandlerByProvider("Delta").get() instanceof DeltaHandler);
  }

  @Test
  void getCatalogHandlerByProviderUnknown() {
    assertEquals(Optional.empty(), CatalogUtils3.getCatalogHandlerByProvider("Unknown"));
  }

  @Test
  void testGetCatalogHandler() {
    TableCatalog tableCatalog = mock(org.apache.iceberg.spark.SparkCatalog.class);
    assertTrue(CatalogUtils3.getCatalogHandler(tableCatalog).get() instanceof IcebergHandler);
  }

  @Test
  void testGetCatalogHandlerEmpty() {
    assertEquals(Optional.empty(), CatalogUtils3.getCatalogHandler(mock(TableCatalog.class)));
  }

  @Test
  void testGetTableProviderFacet() {
    TableCatalog tableCatalog = mock(org.apache.iceberg.spark.SparkCatalog.class);
    Map<String, String> properties = new HashMap<>();
    assertEquals(
        "iceberg",
        CatalogUtils3.getTableProviderFacet(tableCatalog, properties).get().getProvider());
  }

  @Test
  void testGetTableProviderFacetWhenHandlerUnknown() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Map<String, String> properties = new HashMap<>();
    assertEquals(Optional.empty(), CatalogUtils3.getTableProviderFacet(tableCatalog, properties));
  }

  @Test
  void testGetDatasetIdentifier() {
    CatalogHandler catalogHandler = mock(CatalogHandler.class);
    when(catalogHandler.isClass(any())).thenReturn(true);
    when(catalogHandler.getDatasetIdentifier(any(), any(), any(), any()))
        .thenReturn(new DatasetIdentifier("name", "namespace"));

    DatasetIdentifier datasetIdentifier =
        CatalogUtils3.getDatasetIdentifier(
            mock(SparkSession.class),
            mock(TableCatalog.class),
            mock(Identifier.class),
            new HashMap<>(),
            Arrays.asList(catalogHandler));

    assertEquals("name", datasetIdentifier.getName());
    assertEquals("namespace", datasetIdentifier.getNamespace());
  }

  @Test
  void testGetDatasetIdentifierWhenCatalogUnsupported() {
    CatalogHandler catalogHandler = mock(CatalogHandler.class);
    when(catalogHandler.isClass(any())).thenReturn(false);

    assertThrows(
        UnsupportedCatalogException.class,
        () -> {
          CatalogUtils3.getDatasetIdentifier(
              mock(SparkSession.class),
              mock(TableCatalog.class),
              mock(Identifier.class),
              new HashMap<>(),
              Arrays.asList(catalogHandler));
        });
  }
}
