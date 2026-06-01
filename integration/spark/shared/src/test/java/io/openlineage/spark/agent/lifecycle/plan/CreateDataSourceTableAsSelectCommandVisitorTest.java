/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
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
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class CreateDataSourceTableAsSelectCommandVisitorTest {

  private static final String DATABASE = "test_db";
  private static final String TABLE = "test_table";
  private static final String WAREHOUSE_PATH = "/warehouse/test_db/test_table";
  private static final String FILE_SCHEME = "file";
  private static final String HIVE_FRAMEWORK = "hive";
  private static final String HIVE_METASTORE_URI = "hive://localhost:9083";
  private static final String HIVE_SYMLINK_NAME = "test_db.test_table";

  private SparkSession session;
  private OpenLineageContext context;
  private CreateDataSourceTableAsSelectCommandVisitor visitor;
  private CatalogTable catalogTable;
  private CreateDataSourceTableAsSelectCommand command;

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

    visitor = new CreateDataSourceTableAsSelectCommandVisitor(context);

    TableIdentifier tableId = new TableIdentifier(TABLE, Option.apply(DATABASE));
    catalogTable = CatalogTableTestUtils.getCatalogTable(tableId);

    LogicalPlan query = mock(LogicalPlan.class);
    when(query.schema()).thenReturn(catalogTable.schema());
    command =
        new CreateDataSourceTableAsSelectCommand(
            catalogTable,
            SaveMode.Overwrite,
            query,
            ScalaConversionUtils.<String>fromList(Collections.emptyList()));
  }

  @Test
  void testApplyReturnsSingleDatasetWithCreateLifecycleState() {
    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class);
        MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class)) {

      DatasetIdentifier di = new DatasetIdentifier(WAREHOUSE_PATH, FILE_SCHEME);
      pathUtils.when(() -> PathUtils.fromCatalogTable(catalogTable, session)).thenReturn(di);

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

      List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

      assertThat(datasets).hasSize(1);
      assertThat(datasets.get(0).getFacets().getCatalog()).isNull();
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

    // DatasetIdentifier with a TABLE symlink representing the Hive metastore logical name
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
          .when(() -> CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context))
          .thenReturn(Optional.of(hiveFacet));

      List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

      assertThat(datasets).hasSize(1);
      OpenLineage.SymlinksDatasetFacet symlinks = datasets.get(0).getFacets().getSymlinks();
      assertThat(symlinks).isNotNull();
      assertThat(symlinks.getIdentifiers()).hasSize(1);
      assertThat(symlinks.getIdentifiers().get(0).getName()).isEqualTo(HIVE_SYMLINK_NAME);
      assertThat(symlinks.getIdentifiers().get(0).getNamespace()).isEqualTo(HIVE_METASTORE_URI);
      assertThat(symlinks.getIdentifiers().get(0).getType())
          .isEqualTo(DatasetIdentifier.SymlinkType.TABLE.toString());
    }
  }

  @Test
  void testApplyWithSchemaFallbackToQuerySchema() {
    // CatalogTable with empty schema — visitor should fall back to query schema
    TableIdentifier tableId = new TableIdentifier(TABLE, Option.apply(DATABASE));
    CatalogTable emptySchemaTable = CatalogTableTestUtils.getCatalogTable(tableId);

    StructType querySchema = catalogTable.schema(); // non-empty
    LogicalPlan query = mock(LogicalPlan.class);
    when(query.schema()).thenReturn(querySchema);
    CreateDataSourceTableAsSelectCommand cmd =
        new CreateDataSourceTableAsSelectCommand(
            emptySchemaTable,
            SaveMode.Overwrite,
            query,
            ScalaConversionUtils.<String>fromList(Collections.emptyList()));

    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class);
        MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class)) {

      DatasetIdentifier di = new DatasetIdentifier(WAREHOUSE_PATH, FILE_SCHEME);
      pathUtils.when(() -> PathUtils.fromCatalogTable(emptySchemaTable, session)).thenReturn(di);

      List<OpenLineage.OutputDataset> datasets = visitor.apply(cmd);

      assertThat(datasets).hasSize(1);
      assertThat(datasets.get(0).getFacets().getSchema().getFields()).isNotEmpty();
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
