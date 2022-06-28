/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.collection.immutable.Map;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IcebergHandlerTest {

  private IcebergHandler icebergHandler = new IcebergHandler();
  private SparkSession sparkSession = mock(SparkSession.class);
  private RuntimeConfig runtimeConfig = mock(RuntimeConfig.class);

  @ParameterizedTest
  @CsvSource({
    "hdfs://namenode:8020/warehouse,hdfs://namenode:8020,/warehouse/database.schema.table",
    "/tmp/warehouse,file,/tmp/warehouse/database.schema.table"
  })
  void testGetDatasetIdentifierForHadoop(String warehouseConf, String namespace, String name) {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2<>(
                "spark.sql.catalog.test.type",
                "hadoop",
                "spark.sql.catalog.test.warehouse",
                warehouseConf));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database", "schema"}, "table"),
            new HashMap<>());

    assertEquals(name, datasetIdentifier.getName());
    assertEquals(namespace, datasetIdentifier.getNamespace());
  }

  @Test
  void testGetDatasetIdentifierForHive() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2<>(
                "spark.sql.catalog.test.type",
                "hive",
                "spark.sql.catalog.test.uri",
                "thrift://metastore-host:10001"));
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database", "schema"}, "table"),
            new HashMap<>());

    assertEquals("database.schema.table", datasetIdentifier.getName());
    assertEquals("hive://metastore-host:10001", datasetIdentifier.getNamespace());
  }

  @Test
  void testGetTableProviderFacet() {
    Optional<TableProviderFacet> tableProviderFacet =
        icebergHandler.getTableProviderFacet(Collections.singletonMap("format", "iceberg/parquet"));
    assertEquals("iceberg", tableProviderFacet.get().getProvider());
    assertEquals("parquet", tableProviderFacet.get().getFormat());
  }

  @Test
  void testGetTableProviderFacetWhenFormatNotProvided() {
    Optional<TableProviderFacet> tableProviderFacet =
        icebergHandler.getTableProviderFacet(new HashMap<>());
    assertEquals("iceberg", tableProviderFacet.get().getProvider());
    assertEquals("", tableProviderFacet.get().getFormat());
  }

  @Test
  void testGetVersionString() throws NoSuchTableException {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");

    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().currentSnapshot().snapshotId()).thenReturn(1500100900L);

    Optional<String> version =
        icebergHandler.getDatasetVersion(sparkCatalog, identifier, Collections.emptyMap());

    assertTrue(version.isPresent());
    assertEquals(version.get(), "1500100900");
  }
}
