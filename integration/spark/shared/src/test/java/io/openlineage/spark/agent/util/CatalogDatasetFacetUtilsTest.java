/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class CatalogDatasetFacetUtilsTest {

  private static final String HIVE_TYPE = "hive";
  private static final String BIGLAKE_CATALOG_NAME = "my_biglake_catalog";

  private SparkContext sparkContext;
  private OpenLineageContext context;

  @BeforeEach
  void setup() {
    sparkContext = mock(SparkContext.class);

    context =
        OpenLineageContext.builder()
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();
  }

  @Test
  void testGetCatalogDatasetFacetForVanillaHiveReturnsCatalogFacet() {
    // Standard Hive: no BigLake factory class configured.
    // isBigLakeHiveCatalog throws IllegalArgumentException (missing HiveConf ConfVar)
    // and returns false, so the plain Hive path is taken.
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.sql.catalogImplementation", "hive")
            .set("spark.sql.warehouse.dir", "file:///tmp/warehouse")
            .set("spark.sql.hive.metastore.uris", "thrift://localhost:9083");
    Configuration hadoopConf = new Configuration();

    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);

    Optional<OpenLineage.CatalogDatasetFacet> result =
        CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context);

    assertThat(result).isPresent();
    OpenLineage.CatalogDatasetFacet facet = result.get();
    assertThat(facet.getName()).isEqualTo("default");
    assertThat(facet.getType()).isEqualTo(HIVE_TYPE);
    assertThat(facet.getFramework()).isEqualTo(HIVE_TYPE);
    assertThat(facet.getSource()).isEqualTo("spark");
    assertThat(facet.getWarehouseUri()).contains("/tmp/warehouse");
    assertThat(facet.getMetadataUri()).isEqualTo("hive://localhost:9083");
  }

  @Test
  void testGetCatalogDatasetFacetForVanillaHiveReturnsEmptyWhenNoWarehouseConfigured() {
    // No warehouse URI configured at all → getCatalogDatasetFacetForHive returns empty.
    SparkConf sparkConf = new SparkConf().set("spark.sql.catalogImplementation", "hive");
    Configuration hadoopConf = new Configuration();

    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);

    Optional<OpenLineage.CatalogDatasetFacet> result =
        CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context);

    assertThat(result).isEmpty();
  }

  @Test
  void testGetCatalogDatasetFacetForBigLakeReturnsBigLakeFacetWithProjectId() {
    // BigLake path with project ID configured: expects gcp_project_id in catalogProperties.
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.hive.metastore.blms.catalog.default", BIGLAKE_CATALOG_NAME)
            .set("spark.hive.metastore.blms.project.id", "my-gcp-project");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hive.metastore.warehouse.dir", "gs://my-bucket/warehouse");

    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);

    try (MockedStatic<GoogleCloudPlatformUtils> gcpUtils =
        mockStatic(GoogleCloudPlatformUtils.class)) {
      gcpUtils
          .when(() -> GoogleCloudPlatformUtils.isBigLakeHiveCatalog(any(SparkConf.class)))
          .thenReturn(true);

      Optional<OpenLineage.CatalogDatasetFacet> result =
          CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context);

      assertThat(result).isPresent();
      OpenLineage.CatalogDatasetFacet facet = result.get();
      assertThat(facet.getName()).isEqualTo(BIGLAKE_CATALOG_NAME);
      assertThat(facet.getType()).isEqualTo("gcp_lakehouse");
      assertThat(facet.getFramework()).isEqualTo(HIVE_TYPE);
      assertThat(facet.getSource()).isEqualTo("spark");
      assertThat(facet.getWarehouseUri()).contains("my-bucket/warehouse");
      assertThat(facet.getCatalogProperties()).isNotNull();
      assertThat(facet.getCatalogProperties().getAdditionalProperties())
          .containsEntry("gcp_project_id", "my-gcp-project");
    }
  }

  @Test
  void testGetCatalogDatasetFacetForBigLakeReturnsBigLakeFacetWithoutProjectIdWhenNotConfigured() {
    // BigLake path without project ID: catalogProperties should be null / absent.
    SparkConf sparkConf =
        new SparkConf().set("spark.hive.metastore.blms.catalog.default", BIGLAKE_CATALOG_NAME);
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hive.metastore.warehouse.dir", "gs://my-bucket/warehouse");

    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);

    try (MockedStatic<GoogleCloudPlatformUtils> gcpUtils =
        mockStatic(GoogleCloudPlatformUtils.class)) {
      gcpUtils
          .when(() -> GoogleCloudPlatformUtils.isBigLakeHiveCatalog(any(SparkConf.class)))
          .thenReturn(true);

      Optional<OpenLineage.CatalogDatasetFacet> result =
          CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context);

      assertThat(result).isPresent();
      OpenLineage.CatalogDatasetFacet facet = result.get();
      assertThat(facet.getName()).isEqualTo(BIGLAKE_CATALOG_NAME);
      assertThat(facet.getType()).isEqualTo("gcp_lakehouse");
      assertThat(facet.getCatalogProperties()).isNull();
    }
  }

  @Test
  void testGetCatalogDatasetFacetForBigLakeReturnsEmptyWhenCatalogNameNotConfigured() {
    // BigLake detected but spark.hive.metastore.blms.catalog.default not set → returns empty.
    SparkConf sparkConf = new SparkConf(); // no blms.catalog.default
    Configuration hadoopConf = new Configuration();

    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);

    try (MockedStatic<GoogleCloudPlatformUtils> gcpUtils =
        mockStatic(GoogleCloudPlatformUtils.class)) {
      gcpUtils
          .when(() -> GoogleCloudPlatformUtils.isBigLakeHiveCatalog(any(SparkConf.class)))
          .thenReturn(true);

      Optional<OpenLineage.CatalogDatasetFacet> result =
          CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context);

      assertThat(result).isEmpty();
    }
  }
}
