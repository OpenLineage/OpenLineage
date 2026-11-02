/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieBaseRelation;
import org.apache.hudi.HoodieFileIndex;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class LogicalRelationDatasetBuilderTest {

  private static final String SOME_VERSION = "version_1";
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkSession session = mock(SparkSession.class);
  LogicalRelationDatasetBuilder visitor =
      new LogicalRelationDatasetBuilder(
          openLineageContext, DatasetFactory.output(openLineageContext), false);
  OpenLineage.DatasetVersionDatasetFacet facet = mock(OpenLineage.DatasetVersionDatasetFacet.class);
  OpenLineage openLineage = mock(OpenLineage.class);
  LogicalRelation logicalRelation = mock(LogicalRelation.class);
  SparkContext sparkContext = mock(SparkContext.class);
  SessionState sessionState = mock(SessionState.class);

  SparkOpenLineageExtensionVisitorWrapper visitorWrapper =
      mock(SparkOpenLineageExtensionVisitorWrapper.class);

  @BeforeEach
  void setup() {
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(openLineageContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(session));
    when(openLineageContext.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());
    when(openLineageContext.getSparkExtensionVisitorWrapper()).thenReturn(visitorWrapper);
    when(facet.getDatasetVersion()).thenReturn(SOME_VERSION);
    when(session.sessionState()).thenReturn(sessionState);
  }

  @Test
  void testApplyForHadoopFsRelationDatasetVersionFacet() {
    HadoopFsRelation hadoopFsRelation = mock(HadoopFsRelation.class);
    Configuration hadoopConfig = mock(Configuration.class);
    FileIndex fileIndex = mock(FileIndex.class);
    Path path = new Path("/tmp/path1");
    when(logicalRelation.relation()).thenReturn(hadoopFsRelation);
    when(sessionState.newHadoopConfWithOptions(any())).thenReturn(hadoopConfig);
    when(hadoopFsRelation.location()).thenReturn(fileIndex);

    when(fileIndex.rootPaths())
        .thenReturn(
            JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(path, path))
                .asScala()
                .toSeq());
    when(openLineage.newDatasetFacetsBuilder()).thenReturn(new OpenLineage.DatasetFacetsBuilder());
    when(openLineage.newDatasetVersionDatasetFacet(SOME_VERSION)).thenReturn(facet);

    try (MockedStatic mocked = mockStatic(PlanUtils.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(PlanUtils.getDirectoryPaths(any(), eq(hadoopConfig)))
            .thenReturn(Collections.singletonList(new Path("/tmp")));
        when(DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation))
            .thenReturn(Optional.of(SOME_VERSION));

        List<OpenLineage.Dataset> datasets =
            visitor.apply(mock(SparkListenerEvent.class), logicalRelation);
        assertEquals(1, datasets.size());
        OpenLineage.Dataset ds = datasets.get(0);
        assertEquals("/tmp", ds.getName());
        assertEquals(SOME_VERSION, ds.getFacets().getVersion().getDatasetVersion());
      }
    }
  }

  @Test
  void testApplyForCatalogTable() {
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(logicalRelation.catalogTable()).thenReturn(Option.apply(catalogTable));
    DatasetIdentifier di = new DatasetIdentifier("/tmp", "namespace");
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("value", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    try (MockedStatic pathUtils = mockStatic(PathUtils.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(PathUtils.fromCatalogTable(catalogTable, session)).thenReturn(di);
        when(DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation))
            .thenReturn(Optional.of(SOME_VERSION));
        when(logicalRelation.schema()).thenReturn(schema);

        List<OpenLineage.Dataset> datasets =
            visitor.apply(mock(SparkListenerEvent.class), logicalRelation);
        assertEquals(1, datasets.size());
        OpenLineage.Dataset ds = datasets.get(0);
        assertEquals("/tmp", ds.getName());
        assertEquals(2, ds.getFacets().getSchema().getFields().size());
        assertEquals(SOME_VERSION, ds.getFacets().getVersion().getDatasetVersion());
      }
    }
  }

  @Test
  void testApplyForHudiBaseRelationCoversMorRelations() {
    HoodieBaseRelation hudiRelation = mock(HoodieBaseRelation.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("name", StringType$.MODULE$, false, Metadata.empty())
            });

    when(logicalRelation.relation()).thenReturn(hudiRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.empty());
    when(hudiRelation.basePath()).thenReturn(new Path("/tmp/hudi_mor"));
    when(hudiRelation.schema()).thenReturn(schema);
    when(hudiRelation.metaClient()).thenReturn(metaClient);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);

    List<OpenLineage.Dataset> datasets =
        visitor.apply(mock(SparkListenerEvent.class), logicalRelation);

    assertEquals(1, datasets.size());
    OpenLineage.Dataset dataset = datasets.get(0);
    assertEquals("file", dataset.getNamespace());
    assertEquals("/tmp/hudi_mor", dataset.getName());
    assertEquals("TABLE", dataset.getFacets().getDatasetType().getDatasetType());
    assertEquals("MERGE_ON_READ", dataset.getFacets().getDatasetType().getSubType());
    assertNull(dataset.getFacets().getSymlinks());
  }

  @Test
  void testApplyForHudiBaseRelationCoversCowRelations() {
    HoodieBaseRelation hudiRelation = mock(HoodieBaseRelation.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("name", StringType$.MODULE$, false, Metadata.empty())
            });

    when(logicalRelation.relation()).thenReturn(hudiRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.empty());
    when(hudiRelation.basePath()).thenReturn(new Path("/tmp/hudi_cow"));
    when(hudiRelation.schema()).thenReturn(schema);
    when(hudiRelation.metaClient()).thenReturn(metaClient);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);

    List<OpenLineage.Dataset> datasets =
        visitor.apply(mock(SparkListenerEvent.class), logicalRelation);

    assertEquals(1, datasets.size());
    OpenLineage.Dataset dataset = datasets.get(0);
    assertEquals("file", dataset.getNamespace());
    assertEquals("/tmp/hudi_cow", dataset.getName());
    assertEquals("TABLE", dataset.getFacets().getDatasetType().getDatasetType());
    assertEquals("COPY_ON_WRITE", dataset.getFacets().getDatasetType().getSubType());
    assertNull(dataset.getFacets().getSymlinks());
  }

  @Test
  void testApplyForCatalogBackedHudiCowRelationUsesCatalogIdentity() {
    HadoopFsRelation hadoopFsRelation = mock(HadoopFsRelation.class);
    HoodieFileIndex fileIndex = mock(HoodieFileIndex.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(logicalRelation.relation()).thenReturn(hadoopFsRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(logicalRelation.schema()).thenReturn(hudiSchema());
    when(hadoopFsRelation.location()).thenReturn(fileIndex);
    when(fileIndex.metaClient()).thenReturn(metaClient);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);

    OpenLineage.Dataset dataset = applyWithCatalogIdentity(catalogTable);

    assertEquals("file", dataset.getNamespace());
    assertEquals("/warehouse/orders", dataset.getName());
    assertCatalogSymlink(dataset);
    assertEquals("COPY_ON_WRITE", dataset.getFacets().getDatasetType().getSubType());
  }

  @Test
  void testApplyForCatalogBackedHudiMorRelationUsesCatalogIdentity() {
    HoodieBaseRelation hudiRelation = mock(HoodieBaseRelation.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(logicalRelation.relation()).thenReturn(hudiRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(logicalRelation.schema()).thenReturn(hudiSchema());
    when(hudiRelation.metaClient()).thenReturn(metaClient);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);

    OpenLineage.Dataset dataset = applyWithCatalogIdentity(catalogTable);

    assertEquals("/warehouse/orders", dataset.getName());
    assertCatalogSymlink(dataset);
    assertEquals("MERGE_ON_READ", dataset.getFacets().getDatasetType().getSubType());
  }

  @Test
  void testApplyForHudiBaseRelationWithUnresolvableMetadataReturnsNoDataset() {
    HoodieBaseRelation hudiRelation = mock(HoodieBaseRelation.class);
    when(logicalRelation.relation()).thenReturn(hudiRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.empty());
    when(hudiRelation.metaClient()).thenThrow(new IllegalStateException("incompatible Hudi"));

    assertTrue(visitor.apply(mock(SparkListenerEvent.class), logicalRelation).isEmpty());
  }

  @Test
  void testApplyForCatalogBackedHudiRelationWithUnresolvableMetadataFallsBackToCatalog() {
    HoodieBaseRelation hudiRelation = mock(HoodieBaseRelation.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(logicalRelation.relation()).thenReturn(hudiRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(logicalRelation.schema()).thenReturn(hudiSchema());
    when(hudiRelation.metaClient()).thenThrow(new IllegalStateException("incompatible Hudi"));

    OpenLineage.Dataset dataset = applyWithCatalogIdentity(catalogTable);

    assertEquals("/warehouse/orders", dataset.getName());
    assertCatalogSymlink(dataset);
    assertNull(dataset.getFacets().getDatasetType());
  }

  private OpenLineage.Dataset applyWithCatalogIdentity(CatalogTable catalogTable) {
    when(catalogTable.provider()).thenReturn(Option.apply("hudi"));
    DatasetIdentifier di =
        new DatasetIdentifier("/warehouse/orders", "file")
            .withSymlink("db.orders", "hive://metastore:9083", DatasetIdentifier.SymlinkType.TABLE);
    try (MockedStatic pathUtils = mockStatic(PathUtils.class)) {
      when(PathUtils.fromCatalogTable(catalogTable, session)).thenReturn(di);
      List<OpenLineage.Dataset> datasets =
          visitor.apply(mock(SparkListenerEvent.class), logicalRelation);
      assertEquals(1, datasets.size());
      return datasets.get(0);
    }
  }

  private static void assertCatalogSymlink(OpenLineage.Dataset dataset) {
    List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlinks =
        dataset.getFacets().getSymlinks().getIdentifiers();
    assertEquals(1, symlinks.size());
    assertEquals("db.orders", symlinks.get(0).getName());
    assertEquals("hive://metastore:9083", symlinks.get(0).getNamespace());
  }

  private static StructType hudiSchema() {
    return new StructType(
        new StructField[] {new StructField("name", StringType$.MODULE$, false, Metadata.empty())});
  }
}
