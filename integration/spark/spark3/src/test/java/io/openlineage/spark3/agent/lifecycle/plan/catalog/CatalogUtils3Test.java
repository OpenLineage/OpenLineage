/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;

class CatalogUtils3Test {

  @Test
  void testGetCatalogHandlerEmpty() {
    assertEquals(
        Optional.empty(),
        CatalogUtils3.getCatalogHandler(mock(OpenLineageContext.class), mock(TableCatalog.class)));
  }

  @Test
  void testGetStorageDatasetFacetWhenHandlerUnknown() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Map<String, String> properties = new HashMap<>();
    assertEquals(
        Optional.empty(),
        CatalogUtils3.getStorageDatasetFacet(
            mock(OpenLineageContext.class), tableCatalog, properties));
  }

  @Test
  void testGetDatasetIdentifier() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    CatalogHandler catalogHandler = mock(CatalogHandler.class);
    when(catalogHandler.isClass(any())).thenReturn(true);
    when(catalogHandler.getDatasetIdentifier(any(), any(), any(), any()))
        .thenReturn(new DatasetIdentifier("name", "namespace"));
    when(context.getSparkSession()).thenReturn(Optional.of(mock(SparkSession.class)));

    DatasetIdentifier datasetIdentifier =
        CatalogUtils3.getDatasetIdentifier(
            context,
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
              mock(OpenLineageContext.class),
              mock(TableCatalog.class),
              mock(Identifier.class),
              new HashMap<>(),
              Arrays.asList(catalogHandler));
        });
  }
}
