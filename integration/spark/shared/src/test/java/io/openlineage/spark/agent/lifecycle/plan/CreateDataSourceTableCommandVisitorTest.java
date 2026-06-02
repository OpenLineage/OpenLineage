/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.CatalogTableTestUtils;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.CatalogDatasetFacetUtils;
import io.openlineage.spark.agent.util.GoogleCloudPlatformUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class CreateDataSourceTableCommandVisitorTest {

  private static final String DATABASE = "test_db";
  private static final String TABLE = "test_table";
  private static final String WAREHOUSE_PATH = "/warehouse/test_db/test_table";
  private static final String FILE_SCHEME = "file";
  private static final String HIVE_FRAMEWORK = "hive";
  private static final String HIVE_METASTORE_URI = "hive://localhost:9083";
  private static final String HIVE_SYMLINK_NAME = "test_db.test_table";
  private static final String GCP_LAKEHOUSE = "gcp_lakehouse";

  private SparkSession session;
  private OpenLineageContext context;
  private CreateDataSourceTableCommandVisitor visitor;
  private CatalogTable catalogTable;
  private CreateDataSourceTableCommand command;

  @BeforeEach
  void setup() {
    session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());
    when(session.sparkContext()).thenReturn(sparkContext);

    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    context =
        OpenLineageContext.builder()
            .sparkSession(session)
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();

    visitor = new CreateDataSourceTableCommandVisitor(context);

    TableIdentifier tableId = new TableIdentifier(TABLE, Option.apply(DATABASE));
    catalogTable = CatalogTableTestUtils.getCatalogTable(tableId);
    command = new CreateDataSourceTableCommand(catalogTable, false);
  }

  @Test
  void testApplyReturnsSingleDatasetWithCreateLifecycleState() {
    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class);
        MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class)) {

      DatasetIdentifier di = new DatasetIdentifier(WAREHOUSE_PATH, FILE_SCHEME);
      pathUtils.when(() -> PathUtils.fromCatalogTable(catalogTable, session)).thenReturn(di);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(session, catalogTable.identifier()))
          .thenReturn(false);

      List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

      assertThat(datasets).hasSize(1);
      assertThat(datasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange())
          .isEqualTo(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE);
    }
  }

  @Test
  void testApplyWithoutHiveCatalogProducesNoCatalogFacet() {
    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class);
        MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class)) {

      DatasetIdentifier di = new DatasetIdentifier(WAREHOUSE_PATH, FILE_SCHEME);
      pathUtils.when(() -> PathUtils.fromCatalogTable(catalogTable, session)).thenReturn(di);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(session, catalogTable.identifier()))
          .thenReturn(false);

      List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

      assertThat(datasets).hasSize(1);
      assertThat(datasets.get(0).getFacets().getCatalog()).isNull();
    }
  }

  @Test
  void testApplyWithHiveCatalogIncludesCatalogFacet() {
    OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    OpenLineage.CatalogDatasetFacet expectedFacet =
        ol.newCatalogDatasetFacetBuilder()
            .framework(HIVE_FRAMEWORK)
            .source("spark")
            .name("default")
            .type(HIVE_FRAMEWORK)
            .metadataUri(HIVE_METASTORE_URI)
            .warehouseUri("file:/tmp/warehouse")
            .build();

    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class);
        MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class)) {

      DatasetIdentifier di = new DatasetIdentifier(WAREHOUSE_PATH, FILE_SCHEME);
      pathUtils.when(() -> PathUtils.fromCatalogTable(catalogTable, session)).thenReturn(di);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(session, catalogTable.identifier()))
          .thenReturn(true);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context))
          .thenReturn(Optional.of(expectedFacet));

      List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

      assertThat(datasets).hasSize(1);
      OpenLineage.CatalogDatasetFacet catalog = datasets.get(0).getFacets().getCatalog();
      assertThat(catalog).isNotNull();
      assertThat(catalog.getName()).isEqualTo("default");
      assertThat(catalog.getFramework()).isEqualTo(HIVE_FRAMEWORK);
      assertThat(catalog.getType()).isEqualTo(HIVE_FRAMEWORK);
      assertThat(catalog.getMetadataUri()).isEqualTo(HIVE_METASTORE_URI);
      assertThat(catalog.getWarehouseUri()).isEqualTo("file:/tmp/warehouse");
    }
  }

  @Test
  void testApplyWithBigLakeCatalogIncludesBigLakeFacetAndSymlink() {
    OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    // Facet returned by getCatalogDatasetFacetForHive when BigLake is active —
    // GoogleCloudPlatformUtils is mocked so BigLake-extended HiveConf is not needed on classpath.
    OpenLineage.CatalogDatasetFacet bigLakeFacet =
        ol.newCatalogDatasetFacetBuilder()
            .framework(HIVE_FRAMEWORK)
            .source("spark")
            .name("my_biglake_catalog")
            .type(GCP_LAKEHOUSE)
            .warehouseUri("gs://my-bucket/warehouse")
            .catalogProperties(
                ol.newCatalogDatasetFacetCatalogPropertiesBuilder()
                    .put("gcp_project_id", "my-gcp-project")
                    .build())
            .build();

    DatasetIdentifier diWithBigLakeSymlink = new DatasetIdentifier(WAREHOUSE_PATH, FILE_SCHEME);
    diWithBigLakeSymlink.withSymlink(
        HIVE_SYMLINK_NAME, GCP_LAKEHOUSE, DatasetIdentifier.SymlinkType.TABLE);

    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class);
        MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class);
        MockedStatic<GoogleCloudPlatformUtils> gcpUtils =
            mockStatic(GoogleCloudPlatformUtils.class)) {

      pathUtils
          .when(() -> PathUtils.fromCatalogTable(catalogTable, session))
          .thenReturn(diWithBigLakeSymlink);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(session, catalogTable.identifier()))
          .thenReturn(true);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context))
          .thenReturn(Optional.of(bigLakeFacet));
      gcpUtils
          .when(() -> GoogleCloudPlatformUtils.isBigLakeHiveCatalog(any(SparkConf.class)))
          .thenReturn(true);

      List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

      assertThat(datasets).hasSize(1);
      OpenLineage.OutputDataset ds = datasets.get(0);

      OpenLineage.CatalogDatasetFacet catalog = ds.getFacets().getCatalog();
      assertThat(catalog).isNotNull();
      assertThat(catalog.getName()).isEqualTo("my_biglake_catalog");
      assertThat(catalog.getType()).isEqualTo(GCP_LAKEHOUSE);
      assertThat(catalog.getWarehouseUri()).isEqualTo("gs://my-bucket/warehouse");
      assertThat(catalog.getCatalogProperties().getAdditionalProperties())
          .containsEntry("gcp_project_id", "my-gcp-project");

      OpenLineage.SymlinksDatasetFacet symlinks = ds.getFacets().getSymlinks();
      assertThat(symlinks).isNotNull();
      assertThat(symlinks.getIdentifiers()).hasSize(1);
      assertThat(symlinks.getIdentifiers().get(0).getNamespace()).isEqualTo(GCP_LAKEHOUSE);
      assertThat(symlinks.getIdentifiers().get(0).getName()).isEqualTo(HIVE_SYMLINK_NAME);
    }
  }

  @Test
  void testApplyWithHiveCatalogIncludesSymlinkFacet() {
    OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    OpenLineage.CatalogDatasetFacet hiveFacet =
        ol.newCatalogDatasetFacetBuilder()
            .framework(HIVE_FRAMEWORK)
            .source("spark")
            .name("default")
            .type(HIVE_FRAMEWORK)
            .metadataUri(HIVE_METASTORE_URI)
            .warehouseUri("file:/tmp/warehouse")
            .build();

    DatasetIdentifier diWithSymlink = new DatasetIdentifier(WAREHOUSE_PATH, FILE_SCHEME);
    diWithSymlink.withSymlink(
        HIVE_SYMLINK_NAME, HIVE_METASTORE_URI, DatasetIdentifier.SymlinkType.TABLE);

    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class);
        MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class)) {

      pathUtils
          .when(() -> PathUtils.fromCatalogTable(catalogTable, session))
          .thenReturn(diWithSymlink);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(session, catalogTable.identifier()))
          .thenReturn(true);
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context))
          .thenReturn(Optional.of(hiveFacet));

      List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

      assertThat(datasets).hasSize(1);
      OpenLineage.SymlinksDatasetFacet symlinks = datasets.get(0).getFacets().getSymlinks();
      assertThat(symlinks).isNotNull();
      assertThat(symlinks.getIdentifiers()).hasSize(1);
      assertThat(symlinks.getIdentifiers().get(0).getName()).isEqualTo(HIVE_SYMLINK_NAME);
      assertThat(symlinks.getIdentifiers().get(0).getNamespace()).isEqualTo(HIVE_METASTORE_URI);
    }
  }

  @Test
  void testJobNameSuffix() {
    assertThat(visitor.isDefinedAt(command)).isTrue();
    assertThat(visitor.jobNameSuffix(command))
        .isPresent()
        .hasValueSatisfying(suffix -> assertThat(suffix).contains(TABLE));
  }
}
