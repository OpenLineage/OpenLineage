/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import scala.PartialFunction;
import scala.collection.immutable.Map;

class SaveIntoDataSourceCommandVisitorTest {

  SaveIntoDataSourceCommand command = mock(SaveIntoDataSourceCommand.class);
  OpenLineageContext context = mock(OpenLineageContext.class);
  SaveIntoDataSourceCommandVisitor visitor = new SaveIntoDataSourceCommandVisitor(context);
  DatasetFactory datasetFactory = mock(DatasetFactory.class);
  OpenLineage.OutputDataset dataset = mock(OpenLineage.OutputDataset.class);
  SparkSession sparkSession = mock(SparkSession.class);
  SQLContext sqlContext = mock(SQLContext.class);

  SparkOpenLineageExtensionVisitorWrapper wrapper =
      mock(SparkOpenLineageExtensionVisitorWrapper.class);

  SparkListenerEvent event = mock(SparkListenerEvent.class);

  StructType schema =
      new StructType()
          .add(new StructField("key", DataTypes.IntegerType, false, Metadata.empty()))
          .add(new StructField("value", DataTypes.StringType, false, Metadata.empty()));

  Map<String, String> options =
      ScalaConversionUtils.fromJavaMap(Collections.singletonMap("path", "some-path"));

  @BeforeEach
  public void setup() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(context.getSparkExtensionVisitorWrapper()).thenReturn(wrapper);
    when(sparkSession.sqlContext()).thenReturn(sqlContext);
    when(command.options()).thenReturn(options);
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
  }

  @Test
  void testSchemaExtractedFromLocalRelation() {
    Attribute attr1 = mock(Attribute.class);
    when(attr1.dataType()).thenReturn(DataTypes.StringType);
    when(attr1.name()).thenReturn("a");

    Attribute attr2 = mock(Attribute.class);
    when(attr2.dataType()).thenReturn(DataTypes.IntegerType);
    when(attr2.name()).thenReturn("b");

    DeltaDataSource deltaDataSource = mock(DeltaDataSource.class);
    LocalRelation localRelation = mock(LocalRelation.class);
    when(localRelation.output())
        .thenReturn(ScalaConversionUtils.fromList(Arrays.asList(attr1, attr2)).toSeq());
    when(command.dataSource()).thenReturn(deltaDataSource);
    when(command.schema()).thenReturn(null);
    when(command.mode()).thenReturn(SaveMode.Overwrite);
    when(command.query()).thenReturn(localRelation);
    when(localRelation.output())
        .thenReturn(ScalaConversionUtils.fromList(Arrays.asList(attr1, attr2)).toSeq());

    List<OpenLineage.OutputDataset> datasets =
        visitor.apply(mock(SparkListenerEvent.class), command);

    assertEquals(1, datasets.size());
    assertEquals(
        OVERWRITE, datasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange());
    assertEquals("a", datasets.get(0).getFacets().getSchema().getFields().get(0).getName());
    assertEquals("string", datasets.get(0).getFacets().getSchema().getFields().get(0).getType());
    assertEquals("b", datasets.get(0).getFacets().getSchema().getFields().get(1).getName());
    assertEquals("integer", datasets.get(0).getFacets().getSchema().getFields().get(1).getType());
  }

  @Test
  void testJobNameSuffixForDeltaDataSource() {
    CreatableRelationProvider dataSource =
        mock(
            DeltaDataSource.class, withSettings().extraInterfaces(CreatableRelationProvider.class));
    when(command.dataSource()).thenReturn(dataSource);
    when(command.options()).thenReturn(options);

    assertThat(visitor.jobNameSuffix(command)).isPresent().get().isEqualTo("some-path");
  }

  @Test
  void testJobNameSuffixForKustoRelation() {
    CreatableRelationProvider dataSource =
        mock(
            DeltaDataSource.class, withSettings().extraInterfaces(CreatableRelationProvider.class));
    when(command.dataSource()).thenReturn(dataSource);
    try (MockedStatic mock = mockStatic(KustoRelationVisitor.class)) {
      when(KustoRelationVisitor.isKustoSource(dataSource)).thenReturn(true);
      when(command.options())
          .thenReturn(
              (Map<String, String>)
                  ScalaConversionUtils.fromJavaMap(
                      Collections.singletonMap("kustotable", "some-table")));

      assertThat(visitor.jobNameSuffix(command)).isPresent().get().isEqualTo("some-table");
    }
  }

  @Test
  void testJobNameSuffixForRelationProvider() {
    CreatableRelationProvider dataSource =
        mock(
            CreatableRelationProvider.class,
            withSettings().extraInterfaces(RelationProvider.class));
    when(command.dataSource()).thenReturn(dataSource);
    when(command.options())
        .thenReturn(
            (Map<String, String>)
                ScalaConversionUtils.fromJavaMap(
                    Collections.singletonMap("kustotable", "some-table")));

    assertThat(visitor.jobNameSuffix(command)).isPresent().get().isEqualTo("some-table");
  }

  @Test
  void testJdbcRelationProviderAsDatasource() {
    SaveIntoDataSourceCommand command = mock(SaveIntoDataSourceCommand.class);
    when(command.schema()).thenReturn(schema);
    java.util.Map<String, String> options = new java.util.HashMap<>();
    options.put("dbtable", "public.test_table");
    options.put("url", "postgres://127.0.0.1/some_db");
    when(command.options()).thenReturn(ScalaConversionUtils.fromJavaMap(options));
    JdbcRelationProvider provider = mock(JdbcRelationProvider.class);
    when(command.dataSource()).thenReturn(provider);

    List<OpenLineage.OutputDataset> result = visitor.apply(event, command);
    assertEquals(result.size(), 1);
    assertEquals("key", result.get(0).getFacets().getSchema().getFields().get(0).getName());
    assertEquals("integer", result.get(0).getFacets().getSchema().getFields().get(0).getType());
    assertEquals("value", result.get(0).getFacets().getSchema().getFields().get(1).getName());
    assertEquals("string", result.get(0).getFacets().getSchema().getFields().get(1).getType());
    assertEquals("postgres://127.0.0.1:5432", result.get(0).getNamespace());
    assertEquals("some_db.public.test_table", result.get(0).getName());
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "jdbc:postgresql://127.0.0.1/some_db|some_db.public.test_table|some_db.public.test_table",
        "jdbc:sqlserver://127.0.0.1:1433;databaseName=some_db|some_db.dbo.test_table|some_db.dbo.test_table",
        "jdbc:sqlserver://127.0.0.1:1433;databaseName=some_db|other_db.dbo.test_table|other_db.dbo.test_table"
      })
  void testJdbcWriteToFullyQualifiedTable(String url, String dbtable, String expectedName) {
    java.util.Map<String, String> options = new java.util.HashMap<>();
    options.put("dbtable", dbtable);
    options.put("url", url);
    when(command.options()).thenReturn(ScalaConversionUtils.fromJavaMap(options));
    when(command.schema()).thenReturn(schema);
    when(command.dataSource()).thenReturn(mock(JdbcRelationProvider.class));

    List<OpenLineage.OutputDataset> result = visitor.apply(event, command);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getName()).isEqualTo(expectedName);
  }

  @ParameterizedTest
  @MethodSource("lifecycleStateChangeBySaveMode")
  void testJdbcLifecycleStateChange(SaveMode mode, LifecycleStateChange expected) {
    java.util.Map<String, String> options = new java.util.HashMap<>();
    options.put("dbtable", "public.test_table");
    options.put("url", "postgres://127.0.0.1/some_db");
    when(command.options()).thenReturn(ScalaConversionUtils.fromJavaMap(options));
    when(command.schema()).thenReturn(schema);
    when(command.mode()).thenReturn(mode);
    when(command.dataSource()).thenReturn(mock(JdbcRelationProvider.class));

    assertLifecycleStateChange(visitor.apply(event, command), expected);
  }

  @ParameterizedTest
  @MethodSource("lifecycleStateChangeBySaveMode")
  void testDeltaPathLifecycleStateChange(SaveMode mode, LifecycleStateChange expected) {
    when(command.schema()).thenReturn(schema);
    when(command.mode()).thenReturn(mode);
    when(command.dataSource()).thenReturn(mock(DeltaDataSource.class));

    assertLifecycleStateChange(visitor.apply(event, command), expected);
  }

  @ParameterizedTest
  @MethodSource("lifecycleStateChangeBySaveMode")
  @SuppressWarnings("unchecked")
  void testRelationProviderLifecycleStateChange(SaveMode mode, LifecycleStateChange expected) {
    CreatableRelationProvider provider =
        mock(
            CreatableRelationProvider.class,
            withSettings().extraInterfaces(RelationProvider.class));
    when(((RelationProvider) provider).createRelation(sqlContext, options))
        .thenReturn(mock(BaseRelation.class));
    when(command.schema()).thenReturn(schema);
    when(command.mode()).thenReturn(mode);
    when(command.dataSource()).thenReturn(provider);

    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    OpenLineage.OutputDataset relationDataset =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("namespace")
            .name("name")
            .facets(openLineage.newDatasetFacetsBuilder().build())
            .build();
    PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> relationVisitor =
        mock(PartialFunction.class);
    when(relationVisitor.isDefinedAt(any())).thenReturn(true);
    when(relationVisitor.apply(any())).thenReturn(Collections.singletonList(relationDataset));
    when(context.getOutputDatasetQueryPlanVisitors())
        .thenReturn(Collections.singletonList(relationVisitor));
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.emptyList());

    assertLifecycleStateChange(visitor.apply(event, command), expected);
  }

  private static Stream<Arguments> lifecycleStateChangeBySaveMode() {
    return Stream.of(
        Arguments.of(SaveMode.Overwrite, OVERWRITE),
        Arguments.of(SaveMode.ErrorIfExists, CREATE),
        Arguments.of(SaveMode.Append, null),
        Arguments.of(SaveMode.Ignore, null));
  }

  private static void assertLifecycleStateChange(
      List<OpenLineage.OutputDataset> datasets, LifecycleStateChange expected) {
    assertThat(datasets).hasSize(1);
    OpenLineage.LifecycleStateChangeDatasetFacet facet =
        datasets.get(0).getFacets().getLifecycleStateChange();
    if (expected == null) {
      assertThat(facet).isNull();
    } else {
      assertThat(facet.getLifecycleStateChange()).isEqualTo(expected);
    }
  }

  @Test
  void testJobNameSuffixForTableOption() {
    CreatableRelationProvider dataSource = mock(CreatableRelationProvider.class);
    when(command.dataSource()).thenReturn(dataSource);

    when(command.options())
        .thenReturn(
            (Map<String, String>)
                ScalaConversionUtils.fromJavaMap(
                    Collections.singletonMap("table", "my-target-table")));

    assertThat(visitor.jobNameSuffix(command)).isPresent().get().isEqualTo("my-target-table");
  }

  abstract class DeltaDataSource implements CreatableRelationProvider {}
}
