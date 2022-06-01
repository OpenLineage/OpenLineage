/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
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

public class CatalogUtils3Test {

  @Test
  public void testGetCatalogHandlerByProviderForIceberg() {
    assertTrue(
        CatalogUtils3.getCatalogHandlerByProvider("Iceberg").get() instanceof IcebergHandler);
  }

  @Test
  public void testGetCatalogHandlerByProviderForDelta() {
    assertTrue(CatalogUtils3.getCatalogHandlerByProvider("Delta").get() instanceof DeltaHandler);
  }

  @Test
  public void getCatalogHandlerByProviderUnknown() {
    assertEquals(Optional.empty(), CatalogUtils3.getCatalogHandlerByProvider("Unknown"));
  }

  @Test
  public void testGetCatalogHandler() {
    TableCatalog tableCatalog = mock(org.apache.iceberg.spark.SparkCatalog.class);
    assertTrue(CatalogUtils3.getCatalogHandler(tableCatalog).get() instanceof IcebergHandler);
  }

  @Test
  public void testGetCatalogHandlerEmpty() {
    assertEquals(Optional.empty(), CatalogUtils3.getCatalogHandler(mock(TableCatalog.class)));
  }

  @Test
  public void testGetTableProviderFacet() {
    TableCatalog tableCatalog = mock(org.apache.iceberg.spark.SparkCatalog.class);
    Map<String, String> properties = new HashMap<>();
    assertEquals(
        "iceberg",
        CatalogUtils3.getTableProviderFacet(tableCatalog, properties).get().getProvider());
  }

  @Test
  public void testGetTableProviderFacetWhenHandlerUnknown() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Map<String, String> properties = new HashMap<>();
    assertEquals(Optional.empty(), CatalogUtils3.getTableProviderFacet(tableCatalog, properties));
  }

  @Test
  public void testGetDatasetIdentifier() {
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
  public void testGetDatasetIdentifierWhenCatalogUnsupported() {
    CatalogHandler catalogHandler = mock(CatalogHandler.class);
    when(catalogHandler.isClass(any())).thenReturn(false);

    UnsupportedCatalogException exception =
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
