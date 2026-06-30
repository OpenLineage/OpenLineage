/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.util.CatalogDatasetFacetUtils;
import io.openlineage.spark.agent.util.GoogleCloudPlatformUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V2SessionCatalogHandlerTest {
  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final V2SessionCatalogHandler catalogHandler = new V2SessionCatalogHandler(context);
  private final V2SessionCatalog catalog = mock(V2SessionCatalog.class);
  private final SparkSession session = mock(SparkSession.class);
  private final SparkContext sparkContext = mock(SparkContext.class);
  private final Map<String, String> properties =
      Collections.singletonMap("location", "file:/tmp/warehouse/schema.db/table");
  private final Map<String, String> namespaceMetadata =
      Collections.singletonMap("location", "file:/tmp/warehouse");
  private final Identifier id = Identifier.of(new String[] {"catalog", "schema"}, "table");

  @BeforeEach
  void setUp() {
    when(context.getSparkSession()).thenReturn(Optional.of(session));
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(session.sparkContext()).thenReturn(sparkContext);
  }

  @Test
  void testGetNonHiveDatasetIdentifierWhenOnlyTableLocationPresent() {
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(Collections.emptyMap());

    DatasetIdentifier datasetIdentifier =
        catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    // no warehouse -> no symlinks
    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }

  @Test
  void testGetNonHiveDatasetIdentifierWhenWarehouseLocationPresent() {
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(namespaceMetadata);

    DatasetIdentifier datasetIdentifier =
        catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);

    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "catalog.schema.table")
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/warehouse")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }

  @Test
  void testGetHiveDatasetIdentifier() {
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(namespaceMetadata);

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(catalog))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);
      try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class)) {
        URI hiveUri = URI.create("hive://hive-host:9093");
        pathUtils
            .when(() -> PathUtils.getMetastoreUri(sparkContext))
            .thenReturn(Optional.of(hiveUri));
        pathUtils.when(() -> PathUtils.prepareHiveUri(hiveUri)).thenReturn(hiveUri);
        pathUtils
            .when(() -> PathUtils.fromPath(any()))
            .thenReturn(new DatasetIdentifier("/tmp/warehouse/schema.db/table", "file"));

        DatasetIdentifier datasetIdentifier =
            catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

        assertThat(datasetIdentifier)
            .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
            .hasFieldOrPropertyWithValue("namespace", "file");

        assertThat(datasetIdentifier.getSymlinks()).hasSize(1);

        assertThat(datasetIdentifier.getSymlinks().get(0))
            .hasFieldOrPropertyWithValue("name", "catalog.schema.table")
            .hasFieldOrPropertyWithValue("namespace", "hive://hive-host:9093")
            .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
      }
    }
  }

  @Test
  void testGetLakehouseHiveDatasetIdentifier() {
    when(sparkContext.getConf())
        .thenReturn(new SparkConf().set("spark.sql.catalogImplementation", "hive"));
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(namespaceMetadata);

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(catalog))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);
      try (MockedStatic<GoogleCloudPlatformUtils> gcpUtils =
          mockStatic(GoogleCloudPlatformUtils.class)) {
        gcpUtils.when(() -> GoogleCloudPlatformUtils.isBigLakeHiveCatalog(any())).thenReturn(true);

        DatasetIdentifier datasetIdentifier =
            catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

        assertThat(datasetIdentifier)
            .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
            .hasFieldOrPropertyWithValue("namespace", "file");

        assertThat(datasetIdentifier.getSymlinks()).hasSize(1);

        assertThat(datasetIdentifier.getSymlinks().get(0))
            .hasFieldOrPropertyWithValue("name", "catalog.schema.table")
            .hasFieldOrPropertyWithValue("namespace", "gcp_lakehouse")
            .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
      }
    }
  }

  @Test
  void testGetCatalogFacetForNonHive() {
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(catalog))
          .thenReturn(false);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);

      Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
          catalogHandler.getCatalogDatasetFacet(mock(V2SessionCatalog.class), new HashMap<>());
      assertThat(catalogDatasetFacet).isEmpty();
    }
  }

  @Test
  void testGetCatalogFacetForHive() {
    OpenLineage.CatalogDatasetFacet cf = mock(OpenLineage.CatalogDatasetFacet.class);

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(any()))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(any()))
          .thenReturn(Optional.of(cf));
      Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
          catalogHandler.getCatalogDatasetFacet(mock(V2SessionCatalog.class), new HashMap<>());
      assertThat(catalogDatasetFacet).isNotEmpty();
      assertThat(catalogDatasetFacet.get().getCatalogDatasetFacet()).isEqualTo(cf);
    }
  }

  @Test
  void testGetDatasetIdentifierWhenNoTableLocationPresent() {
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(Collections.emptyMap());

    OpenLineageClientException thrown =
        assertThrows(
            OpenLineageClientException.class,
            () -> catalogHandler.getDatasetIdentifier(session, catalog, id, new HashMap<>()));

    assertThat(thrown.getMessage())
        .contains("Unable to extract DatasetIdentifier from V2SessionCatalog");
  }
}
