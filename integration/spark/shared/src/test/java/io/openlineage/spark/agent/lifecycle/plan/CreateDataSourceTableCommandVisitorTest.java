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
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.List;
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
  void testJobNameSuffix() {
    assertThat(visitor.isDefinedAt(command)).isTrue();
    assertThat(visitor.jobNameSuffix(command))
        .isPresent()
        .hasValueSatisfying(suffix -> assertThat(suffix).contains(TABLE));
  }
}
