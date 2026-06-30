/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.DatasetBuilderFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;

class CatalogUtilsTest {

  /** Builds a context whose factory contributes exactly the given catalog handlers. */
  private static OpenLineageContext contextWithHandlers(CatalogHandler... handlers) {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getDatasetBuilderFactory())
        .thenReturn(
            new DatasetBuilderFactory() {
              @Override
              public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>>
                  getInputBuilders(OpenLineageContext ctx) {
                return Collections.emptyList();
              }

              @Override
              public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
                  getOutputBuilders(OpenLineageContext ctx) {
                return Collections.emptyList();
              }

              @Override
              public List<CatalogHandler> getCatalogHandlers(OpenLineageContext ctx) {
                return Arrays.asList(handlers);
              }
            });
    return context;
  }

  /**
   * Mirrors a real context built by the agent's ContextFactory, where {@code datasetBuilderFactory}
   * defaults to {@link DatasetBuilderFactory#EMPTY} (no contributed handlers).
   */
  private static OpenLineageContext emptyContext() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getDatasetBuilderFactory()).thenReturn(DatasetBuilderFactory.EMPTY);
    return context;
  }

  @Test
  void testGetCatalogHandlerReturnsTheMatchingHandler() {
    CatalogHandler nonMatching = mock(CatalogHandler.class);
    when(nonMatching.hasClasses()).thenReturn(true);
    when(nonMatching.isClass(any())).thenReturn(false);

    CatalogHandler matching = mock(CatalogHandler.class);
    when(matching.hasClasses()).thenReturn(true);
    when(matching.isClass(any())).thenReturn(true);

    OpenLineageContext context = contextWithHandlers(nonMatching, matching);

    assertEquals(matching, CatalogUtils.getCatalogHandler(context, mock(TableCatalog.class)).get());
  }

  @Test
  void testGetCatalogHandlerEmpty() {
    assertEquals(
        Optional.empty(), CatalogUtils.getCatalogHandler(emptyContext(), mock(TableCatalog.class)));
  }

  @Test
  void testGetCatalogHandlerIncludesFactoryContributedHandlers() {
    CatalogHandler contributed = mock(CatalogHandler.class);
    when(contributed.hasClasses()).thenReturn(true);
    when(contributed.isClass(any())).thenReturn(true);

    OpenLineageContext context = contextWithHandlers(contributed);

    assertEquals(
        contributed, CatalogUtils.getCatalogHandler(context, mock(TableCatalog.class)).get());
  }

  @Test
  void testGetStorageDatasetFacetDelegatesToMatchingHandler() {
    OpenLineage.StorageDatasetFacet facet = mock(OpenLineage.StorageDatasetFacet.class);

    CatalogHandler handler = mock(CatalogHandler.class);
    when(handler.hasClasses()).thenReturn(true);
    when(handler.isClass(any())).thenReturn(true);
    when(handler.getStorageDatasetFacet(any())).thenReturn(Optional.of(facet));

    OpenLineageContext context = contextWithHandlers(handler);

    assertEquals(
        facet,
        CatalogUtils.getStorageDatasetFacet(context, mock(TableCatalog.class), new HashMap<>())
            .get());
  }

  @Test
  void testGetStorageDatasetFacetWhenHandlerUnknown() {
    assertEquals(
        Optional.empty(),
        CatalogUtils.getStorageDatasetFacet(
            emptyContext(), mock(TableCatalog.class), new HashMap<>()));
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
        CatalogUtils.getDatasetIdentifier(
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
        () ->
            CatalogUtils.getDatasetIdentifier(
                mock(OpenLineageContext.class),
                mock(TableCatalog.class),
                mock(Identifier.class),
                new HashMap<>(),
                Arrays.asList(catalogHandler)));
  }
}
