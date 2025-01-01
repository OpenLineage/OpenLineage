/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class DataSourceV2RelationDatasetExtractorTest {
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkSession sparkSession = mock(SparkSession.class);
  DatasetFactory<OpenLineage.Dataset> datasetFactory = mock(DatasetFactory.class);
  DataSourceV2Relation dataSourceV2Relation = mock(DataSourceV2Relation.class);
  DatasetCompositeFacetsBuilder datasetFacetsBuilder = mock(DatasetCompositeFacetsBuilder.class);
  TableCatalog tableCatalog = mock(TableCatalog.class);
  Identifier identifier = mock(Identifier.class);
  StructType schema = mock(StructType.class);
  Table table = mock(Table.class);
  Map<String, String> tableProperties;
  OpenLineage openLineage = mock(OpenLineage.class);

  @BeforeEach
  void setUp() {
    tableProperties = new HashMap<>();
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(openLineageContext.getOpenLineage()).thenReturn(openLineage);
    when(dataSourceV2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(dataSourceV2Relation.schema()).thenReturn(schema);
    when(dataSourceV2Relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);
    when(datasetFactory.createCompositeFacetBuilder()).thenReturn(datasetFacetsBuilder);
  }

  @Test
  void testExtractFromDataSourceV2Relation() {
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      try (MockedStatic<PlanUtils> mockedPlanUtils = mockStatic(PlanUtils.class)) {
        DatasetIdentifier di = mock(DatasetIdentifier.class);
        when(di.getNamespace()).thenReturn("file://tmp");
        when(di.getName()).thenReturn("name");

        OpenLineage.DatasetFacets datasetFacets = mock(OpenLineage.DatasetFacets.class);
        OpenLineage.Dataset dataset = mock(OpenLineage.Dataset.class);
        OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
            mock(OpenLineage.SchemaDatasetFacet.class);
        OpenLineage.DatasourceDatasetFacet datasourceDatasetFacet =
            mock(OpenLineage.DatasourceDatasetFacet.class);
        when(PlanUtils.schemaFacet(openLineage, schema)).thenReturn(schemaDatasetFacet);
        when(PlanUtils.datasourceFacet(openLineage, di.getNamespace()))
            .thenReturn(datasourceDatasetFacet);

        DatasetFacetsBuilder facetsBuilder = mock(DatasetFacetsBuilder.class);
        when(datasetFacetsBuilder.getFacets()).thenReturn(facetsBuilder);

        when(facetsBuilder.schema(schemaDatasetFacet)).thenReturn(facetsBuilder);
        when(facetsBuilder.dataSource(datasourceDatasetFacet)).thenReturn(facetsBuilder);
        when(facetsBuilder.build()).thenReturn(datasetFacets);

        when(CatalogUtils3.getDatasetIdentifier(
                openLineageContext, tableCatalog, identifier, tableProperties))
            .thenReturn(di);
        when(datasetFactory.getDataset(di, datasetFacetsBuilder)).thenReturn(dataset);

        assertEquals(
            Collections.singletonList(dataset),
            DataSourceV2RelationDatasetExtractor.extract(
                datasetFactory, openLineageContext, dataSourceV2Relation));
      }
    }
  }

  @Test
  void testExtractFromDataSourceV2RelationWhenDatasetIdentifierEmpty() {
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      DatasetIdentifier di = mock(DatasetIdentifier.class);
      OpenLineage.Dataset dataset = mock(OpenLineage.Dataset.class);

      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenThrow(new UnsupportedCatalogException("exception"));
      when(datasetFactory.getDataset(di, schema)).thenReturn(dataset);

      assertEquals(
          Collections.emptyList(),
          DataSourceV2RelationDatasetExtractor.extract(
              datasetFactory, openLineageContext, dataSourceV2Relation));
    }
  }

  @Test
  void testExtractFromDataSourceV2RelationWhenIdentifierEmpty() {
    when(dataSourceV2Relation.identifier()).thenReturn(Option.empty());
    final List<OpenLineage.Dataset> result =
        DataSourceV2RelationDatasetExtractor.extract(
            datasetFactory, openLineageContext, dataSourceV2Relation);
    assertEquals(0, result.size());
  }

  @Test
  void testExtractFromDataSourceV2RelationWhenCatalogEmpty() {
    when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(mock(Identifier.class)));
    when(dataSourceV2Relation.catalog()).thenReturn(Option.empty());
    final List<OpenLineage.Dataset> result =
        DataSourceV2RelationDatasetExtractor.extract(
            datasetFactory, openLineageContext, dataSourceV2Relation);
    assertEquals(0, result.size());
  }

  @Test
  void testGetDatasetIdentifierFromV2Relation() {
    DatasetIdentifier di = mock(DatasetIdentifier.class);
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(di);
      assertEquals(
          di,
          DataSourceV2RelationDatasetExtractor.getDatasetIdentifier(
                  openLineageContext, dataSourceV2Relation)
              .get());
    }
  }

  @Test
  void testGetDatasetIdentifierFromV2RelationWithMissingIdentifier() {
    when(dataSourceV2Relation.identifier()).thenReturn(null).thenReturn(Option.empty());
    assertEquals(
        Optional.empty(),
        DataSourceV2RelationDatasetExtractor.getDatasetIdentifier(
            openLineageContext, dataSourceV2Relation));
  }

  @Test
  void testGetDatasetIdentifierFromV2RelationWithMissingCatalog() {
    when(dataSourceV2Relation.catalog())
        .thenReturn(null)
        .thenReturn(Option.empty())
        .thenReturn(Option.apply(mock(CatalogPlugin.class)));

    assertEquals(
        Optional.empty(),
        DataSourceV2RelationDatasetExtractor.getDatasetIdentifier(
            openLineageContext, dataSourceV2Relation));
  }

  @Test
  void testExtractFromDataSourceV2RelationForExtensionLineage() throws URISyntaxException {
    Map<String, String> properties = new HashMap<>();
    properties.put("openlineage.dataset.name", "some-name");
    properties.put("openlineage.dataset.namespace", "some-namespace");
    properties.put(
        "openlineage.dataset.facets.customFacet",
        "{"
            + "\"property\": \"value\","
            + "\"_producer\": \"https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client\""
            + "}");

    StructType schema =
        new StructType(
            new StructField[] {new StructField("key", IntegerType$.MODULE$, false, null)});

    when(dataSourceV2Relation.schema()).thenReturn(schema);
    when(table.properties()).thenReturn(properties);
    when(openLineageContext.getOpenLineage())
        .thenReturn(
            new OpenLineage(
                new URI("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client")));

    DatasetFactory<OpenLineage.OutputDataset> datasetFactory =
        DatasetFactory.output(openLineageContext);

    final List<OpenLineage.OutputDataset> result =
        DataSourceV2RelationDatasetExtractor.extract(
            datasetFactory, openLineageContext, dataSourceV2Relation);

    assertEquals(1, result.size());
    assertThat(result.get(0))
        .hasFieldOrPropertyWithValue("name", "some-name")
        .hasFieldOrPropertyWithValue("namespace", "some-namespace");

    OpenLineage.DatasetFacet datasetFacet =
        result.get(0).getFacets().getAdditionalProperties().get("customFacet");
    assertThat(datasetFacet.getAdditionalProperties())
        .hasFieldOrPropertyWithValue("property", "value");
  }
}
