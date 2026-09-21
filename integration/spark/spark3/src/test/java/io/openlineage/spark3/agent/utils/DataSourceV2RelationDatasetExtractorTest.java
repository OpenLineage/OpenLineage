/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.DatasetBuilderFactory;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.lifecycle.plan.catalog.RelationHandler;
import io.openlineage.spark.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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
import scala.PartialFunction;

class DataSourceV2RelationDatasetExtractorTest {
  private static final String NAME = "name";
  private static final String NAMESPACE = "namespace";
  private static final String CATALOG_DB_TABLE = "catalog.db.table";

  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkSession sparkSession = mock(SparkSession.class);
  DatasetFactory<Dataset> datasetFactory = mock(DatasetFactory.class);
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
    when(openLineageContext.getDatasetBuilderFactory()).thenReturn(DatasetBuilderFactory.EMPTY);
    when(dataSourceV2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(dataSourceV2Relation.schema()).thenReturn(schema);
    when(dataSourceV2Relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);
    when(datasetFactory.createCompositeFacetBuilder()).thenReturn(datasetFacetsBuilder);
  }

  @Test
  void testExtractFromDataSourceV2Relation() {
    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      try (MockedStatic<PlanUtils> mockedPlanUtils = mockStatic(PlanUtils.class)) {
        DatasetIdentifier di = mock(DatasetIdentifier.class);
        when(di.getNamespace()).thenReturn("file://tmp");
        when(di.getName()).thenReturn(NAME);

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

        when(CatalogUtils.getDatasetIdentifier(
                openLineageContext, tableCatalog, identifier, tableProperties))
            .thenReturn(di);

        SparkDatasetBuilder sparkBuilder = mock(SparkDatasetBuilder.class);
        when(datasetFactory.sparkDatasetBuilder(datasetFacetsBuilder)).thenReturn(sparkBuilder);
        when(sparkBuilder.dataset(any(DatasetIdentifier.class))).thenReturn(sparkBuilder);
        when(sparkBuilder.build()).thenReturn(dataset);

        assertEquals(
            Collections.singletonList(dataset),
            DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
                datasetFactory, openLineageContext, dataSourceV2Relation));
      }
    }
  }

  @Test
  void testExtractFromDataSourceV2RelationWhenDatasetIdentifierEmpty() {
    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      when(CatalogUtils.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenThrow(new UnsupportedCatalogException("exception"));
      // the relation cannot be resolved either, so there is nothing left to fall back to
      when(CatalogUtils.getDatasetIdentifierFromRelation(openLineageContext, dataSourceV2Relation))
          .thenThrow(new UnsupportedCatalogException("exception"));

      assertEquals(
          Collections.emptyList(),
          DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
              datasetFactory, openLineageContext, dataSourceV2Relation));
    }
  }

  @Test
  void testExtractFromDataSourceV2RelationWhenIdentifierEmpty() {
    when(dataSourceV2Relation.identifier()).thenReturn(Option.empty());
    final List<OpenLineage.Dataset> result =
        DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
            datasetFactory, openLineageContext, dataSourceV2Relation);
    assertEquals(0, result.size());
  }

  @Test
  void testExtractFromDataSourceV2RelationWhenCatalogEmpty() {
    when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(mock(Identifier.class)));
    when(dataSourceV2Relation.catalog()).thenReturn(Option.empty());
    final List<OpenLineage.Dataset> result =
        DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
            datasetFactory, openLineageContext, dataSourceV2Relation);
    assertEquals(0, result.size());
  }

  @Test
  void testGetDatasetIdentifierFromV2Relation() {
    DatasetIdentifier di = mock(DatasetIdentifier.class);
    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      when(CatalogUtils.getDatasetIdentifier(
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
        DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
            datasetFactory, openLineageContext, dataSourceV2Relation);

    assertEquals(1, result.size());
    assertThat(result.get(0))
        .hasFieldOrPropertyWithValue(NAME, "some-name")
        .hasFieldOrPropertyWithValue(NAMESPACE, "some-namespace");

    OpenLineage.DatasetFacet datasetFacet =
        result.get(0).getFacets().getAdditionalProperties().get("customFacet");
    assertThat(datasetFacet.getAdditionalProperties())
        .hasFieldOrPropertyWithValue("property", "value");
  }

  @Test
  void testExtractFromDataSourceV2RelationForExtensionLineageWithQuery() throws URISyntaxException {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        "openlineage.dataset.query",
        "SELECT column1, column2 FROM `bigquery-public-data.samples.shakespeare`");
    properties.put("openlineage.dataset.namespace", "bigquery");
    properties.put(
        "openlineage.dataset.facets.customFacet",
        "{"
            + "\"property\": \"value\","
            + "\"_producer\": \"https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client\""
            + "}");

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("column1", IntegerType$.MODULE$, false, null),
              new StructField("column2", IntegerType$.MODULE$, false, null)
            });

    when(dataSourceV2Relation.schema()).thenReturn(schema);
    when(table.properties()).thenReturn(properties);
    when(openLineageContext.getOpenLineage())
        .thenReturn(
            new OpenLineage(
                new URI("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client")));

    DatasetFactory<OpenLineage.OutputDataset> datasetFactory =
        DatasetFactory.output(openLineageContext);

    final List<OpenLineage.OutputDataset> result =
        DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
            datasetFactory, openLineageContext, dataSourceV2Relation);

    assertEquals(1, result.size());
    assertThat(result.get(0))
        .hasFieldOrPropertyWithValue(NAME, "bigquery-public-data.samples.shakespeare")
        .hasFieldOrPropertyWithValue(NAMESPACE, "bigquery");
  }

  @Test
  void testExtractFallsBackToUnityCatalogNameWhenLocationResolutionFails() {
    SparkConf sparkConf = new SparkConf();
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(openLineageContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(dataSourceV2Relation.schema()).thenReturn(new StructType());

    try (MockedStatic<PlanUtils3> planUtils = mockStatic(PlanUtils3.class)) {
      try (MockedStatic<DatabricksUtils> databricks = mockStatic(DatabricksUtils.class)) {
        try (MockedStatic<PlanUtils> mockedPlanUtils = mockStatic(PlanUtils.class)) {
          databricks
              .when(() -> DatabricksUtils.isDatabricksUnityCatalogEnabled(sparkConf))
              .thenReturn(true);
          databricks
              .when(() -> DatabricksUtils.qualifiedUnityCatalogTableName(tableCatalog, identifier))
              .thenReturn(CATALOG_DB_TABLE);
          planUtils
              .when(
                  () ->
                      PlanUtils3.getDatasetIdentifier(
                          openLineageContext, tableCatalog, identifier, tableProperties))
              .thenThrow(
                  new IllegalStateException("no default path for a unity catalog namespace"));

          DatasetFactory<OpenLineage.OutputDataset> outputFactory =
              DatasetFactory.output(openLineageContext);
          List<OpenLineage.OutputDataset> result =
              DataSourceV2RelationDatasetExtractor.extract(
                  outputFactory, openLineageContext, dataSourceV2Relation, false);

          assertEquals(1, result.size());
          assertThat(result.get(0))
              .hasFieldOrPropertyWithValue(NAME, CATALOG_DB_TABLE)
              .hasFieldOrPropertyWithValue(NAMESPACE, "unity-catalog");
        }
      }
    }
  }

  /**
   * The relation fallback must not steal the Unity Catalog fallback's turn. A catalog that does
   * have a {@link io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler} - a real Unity
   * Catalog does - keeps naming its tables the Unity Catalog way even when a relation handler could
   * have produced something too.
   */
  @Test
  void testUnityCatalogFallbackWinsOverRelationFallbackForSupportedCatalog() {
    SparkConf sparkConf = new SparkConf();
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(openLineageContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(dataSourceV2Relation.schema()).thenReturn(new StructType());

    // a handler exists for this catalog, it just cannot resolve this particular table
    CatalogHandler supportingHandler = mock(CatalogHandler.class);
    when(supportingHandler.hasClasses()).thenReturn(true);
    when(supportingHandler.isClass(tableCatalog)).thenReturn(true);
    RelationHandler relationHandler = alwaysResolvingRelationHandler();
    DatasetBuilderFactory factory =
        factoryContributing(
            Collections.singletonList(supportingHandler),
            Collections.singletonList(relationHandler));
    when(openLineageContext.getDatasetBuilderFactory()).thenReturn(factory);

    try (MockedStatic<PlanUtils3> planUtils = mockStatic(PlanUtils3.class);
        MockedStatic<DatabricksUtils> databricks = mockStatic(DatabricksUtils.class);
        MockedStatic<PlanUtils> mockedPlanUtils = mockStatic(PlanUtils.class)) {
      databricks
          .when(() -> DatabricksUtils.isDatabricksUnityCatalogEnabled(sparkConf))
          .thenReturn(true);
      databricks
          .when(() -> DatabricksUtils.qualifiedUnityCatalogTableName(tableCatalog, identifier))
          .thenReturn(CATALOG_DB_TABLE);
      planUtils
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.empty());

      List<OpenLineage.OutputDataset> result =
          DataSourceV2RelationDatasetExtractor.extract(
              DatasetFactory.output(openLineageContext),
              openLineageContext,
              dataSourceV2Relation,
              false);

      assertEquals(1, result.size());
      assertThat(result.get(0))
          .hasFieldOrPropertyWithValue(NAME, CATALOG_DB_TABLE)
          .hasFieldOrPropertyWithValue(NAMESPACE, "unity-catalog");
    }
  }

  /** A relation handler that recognises everything and always resolves. */
  private RelationHandler alwaysResolvingRelationHandler() {
    RelationHandler handler = mock(RelationHandler.class);
    when(handler.hasClasses()).thenReturn(true);
    when(handler.isClass(dataSourceV2Relation)).thenReturn(true);
    when(handler.getDatasetIdentifier(dataSourceV2Relation))
        .thenReturn(new DatasetIdentifier("from-relation", "relation-namespace"));
    return handler;
  }

  /**
   * The cached-catalog case: no handler supports the relation's own catalog, so storage and catalog
   * facets are resolved against the catalog that owns the table instead of coming back empty.
   */
  @Test
  void testFacetsAreResolvedThroughOwningCatalogWhenCatalogUnsupported() {
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(dataSourceV2Relation.schema()).thenReturn(new StructType());

    TableCatalog owningCatalog = mock(TableCatalog.class);
    Identifier owningIdentifier = mock(Identifier.class);
    RelationHandler relationHandler = mock(RelationHandler.class);
    when(relationHandler.hasClasses()).thenReturn(true);
    when(relationHandler.isClass(dataSourceV2Relation)).thenReturn(true);
    when(relationHandler.getOwningCatalog(dataSourceV2Relation))
        .thenReturn(Optional.of(RelationHandler.OwningCatalog.of(owningCatalog, owningIdentifier)));
    when(openLineageContext.getDatasetBuilderFactory())
        .thenReturn(
            factoryContributing(
                Collections.emptyList(), Collections.singletonList(relationHandler)));

    try (MockedStatic<CatalogUtils> catalogUtils = mockStatic(CatalogUtils.class);
        MockedStatic<PlanUtils3> planUtils = mockStatic(PlanUtils3.class)) {
      catalogUtils
          .when(() -> CatalogUtils.getCatalogHandler(openLineageContext, tableCatalog))
          .thenReturn(Optional.empty());
      catalogUtils
          .when(
              () ->
                  CatalogUtils.getOwningCatalogFromRelation(
                      openLineageContext, dataSourceV2Relation))
          .thenReturn(
              Optional.of(RelationHandler.OwningCatalog.of(owningCatalog, owningIdentifier)));
      catalogUtils
          .when(
              () ->
                  CatalogUtils.getDatasetIdentifierFromRelation(
                      openLineageContext, dataSourceV2Relation))
          .thenReturn(new DatasetIdentifier("/warehouse/db/table", "file"));
      planUtils
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.empty());

      List<OpenLineage.OutputDataset> result =
          DataSourceV2RelationDatasetExtractor.extract(
              DatasetFactory.output(openLineageContext),
              openLineageContext,
              dataSourceV2Relation,
              false);

      assertEquals(1, result.size());
      assertThat(result.get(0)).hasFieldOrPropertyWithValue(NAME, "/warehouse/db/table");
      // the facets were resolved against the owning catalog, not the relation's unsupported one
      catalogUtils.verify(
          () ->
              CatalogUtils.addStorageAndCatalogFacets(
                  eq(openLineageContext),
                  eq(owningCatalog),
                  eq(tableProperties),
                  any(DatasetCompositeFacetsBuilder.class)));
    }
  }

  private static DatasetBuilderFactory factoryContributing(
      List<CatalogHandler> catalogHandlers, List<RelationHandler> relationHandlers) {
    return new DatasetBuilderFactory() {
      @Override
      public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
          OpenLineageContext context) {
        return Collections.emptyList();
      }

      @Override
      public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
          OpenLineageContext context) {
        return Collections.emptyList();
      }

      @Override
      public List<CatalogHandler> getCatalogHandlers(OpenLineageContext context) {
        return catalogHandlers;
      }

      @Override
      public List<RelationHandler> getRelationHandlers(OpenLineageContext context) {
        return relationHandlers;
      }
    };
  }

  @Test
  void testExtractFromDataSourceV2RelationContainsVersionFacet() {
    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      try (MockedStatic<PlanUtils> mockedPlanUtils = mockStatic(PlanUtils.class)) {
        try (MockedStatic<DatasetVersionDatasetFacetUtils> versionUtils =
            mockStatic(DatasetVersionDatasetFacetUtils.class)) {
          DatasetFactory<OpenLineage.OutputDataset> datasetFactory =
              DatasetFactory.output(openLineageContext);
          DatasetIdentifier di = mock(DatasetIdentifier.class);
          when(di.getNamespace()).thenReturn("file://tmp");
          when(di.getName()).thenReturn("dataset-name");
          when(openLineageContext.getOpenLineage())
              .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));

          when(CatalogUtils.getDatasetIdentifier(
                  openLineageContext, tableCatalog, identifier, tableProperties))
              .thenReturn(di);
          when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
                  openLineageContext, dataSourceV2Relation))
              .thenReturn(Optional.of("1.0.0"));
          when(dataSourceV2Relation.schema()).thenReturn(new StructType());

          assertThat(
                  DataSourceV2RelationDatasetExtractor.extract(
                          datasetFactory, openLineageContext, dataSourceV2Relation, false)
                      .get(0)
                      .getFacets()
                      .getVersion())
              .isNull();

          assertThat(
                  DataSourceV2RelationDatasetExtractor.extract(
                          datasetFactory, openLineageContext, dataSourceV2Relation, true)
                      .get(0)
                      .getFacets()
                      .getVersion()
                      .getDatasetVersion())
              .isEqualTo("1.0.0");
        }
      }
    }
  }
}
