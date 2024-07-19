/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class PlanUtils3Test {

  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkSession sparkSession = mock(SparkSession.class);
  DataSourceV2Relation dataSourceV2Relation = mock(DataSourceV2Relation.class);
  TableCatalog tableCatalog = mock(TableCatalog.class);
  Identifier identifier = mock(Identifier.class);
  Map<String, String> tableProperties;
  OpenLineage openLineage = mock(OpenLineage.class);

  @BeforeEach
  void setUp() {
    tableProperties = new HashMap<>();
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(openLineageContext.getOpenLineage()).thenReturn(openLineage);
    when(dataSourceV2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(identifier));
  }

  @Test
  void testGetDatasetIdentifier() {
    DatasetIdentifier di = mock(DatasetIdentifier.class);
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(di);

      assertEquals(
          di,
          PlanUtils3.getDatasetIdentifier(
                  openLineageContext, tableCatalog, identifier, tableProperties)
              .get());
    }
  }

  @Test
  void testGetDatasetIdentifierWhenCatalogUnsupported() {
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenThrow(new UnsupportedCatalogException("exception"));

      assertEquals(
          Optional.empty(),
          PlanUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties));
    }
  }

  @Test
  void testGetDatasetIdentifierWhenNoSparkSession() {
    when(openLineageContext.getSparkSession()).thenReturn(Optional.empty());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PlanUtils3.getDatasetIdentifier(
                openLineageContext, tableCatalog, identifier, tableProperties));
  }
}
