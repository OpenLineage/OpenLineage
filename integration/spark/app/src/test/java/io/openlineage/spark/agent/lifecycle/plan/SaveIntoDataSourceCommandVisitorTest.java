/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.scala.v1.LineageRelationProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

class SaveIntoDataSourceCommandVisitorTest {

  SaveIntoDataSourceCommand command = mock(SaveIntoDataSourceCommand.class);
  OpenLineageContext context = mock(OpenLineageContext.class);
  SaveIntoDataSourceCommandVisitor visitor = new SaveIntoDataSourceCommandVisitor(context);
  DatasetFactory datasetFactory = mock(DatasetFactory.class);
  OpenLineage.OutputDataset dataset = mock(OpenLineage.OutputDataset.class);
  SparkSession sparkSession = mock(SparkSession.class);
  SQLContext sqlContext = mock(SQLContext.class);
  Map<String, String> options =
      ScalaConversionUtils.fromJavaMap(Collections.singletonMap("path", "some-path"));

  @BeforeEach
  public void setup() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(sparkSession.sqlContext()).thenReturn(sqlContext);
    when(command.options()).thenReturn(options);
  }

  @Test
  void testSchemaExtractedFromLocalRelation() {
    Attribute attr1 = mock(Attribute.class);
    when(attr1.dataType()).thenReturn(StringType$.MODULE$);
    when(attr1.name()).thenReturn("a");

    Attribute attr2 = mock(Attribute.class);
    when(attr2.dataType()).thenReturn(IntegerType$.MODULE$);
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
    when(datasetFactory.getDataset(any(), any(), eq(OVERWRITE))).thenReturn(dataset);

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
  void testSchemaExtractedFromLineageRelationProvider() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("field", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    LineageRelationProvider provider =
        mock(
            LineageRelationProvider.class,
            withSettings().extraInterfaces(CreatableRelationProvider.class));
    DatasetIdentifier identifier = new DatasetIdentifier("name", "namespace");
    when(command.dataSource()).thenReturn((CreatableRelationProvider) provider);
    when(command.schema()).thenReturn(schema);
    when(provider.getLineageDatasetIdentifier(any(), eq(sqlContext), eq(options)))
        .thenReturn(identifier);

    List<OutputDataset> datasets = visitor.apply(mock(SparkListenerEvent.class), command);
    assertThat(datasets).hasSize(1);
    assertThat(datasets.get(0))
        .hasFieldOrPropertyWithValue("name", "name")
        .hasFieldOrPropertyWithValue("namespace", "namespace");
    assertThat(datasets.get(0).getFacets().getSchema().getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "field");
  }

  @Test
  void testIsDefinedAtLogicalPlanForLineageRelationProvider() {
    LineageRelationProvider provider =
        mock(
            LineageRelationProvider.class,
            withSettings().extraInterfaces(CreatableRelationProvider.class));
    when(command.dataSource()).thenReturn((CreatableRelationProvider) provider);
    assertThat(visitor.isDefinedAtLogicalPlan(command)).isTrue();
  }

  abstract class DeltaDataSource implements CreatableRelationProvider {}
}
