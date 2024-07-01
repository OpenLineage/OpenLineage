/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.shaded.spark.extension.client.utils.DatasetIdentifier;
import io.openlineage.shaded.spark.extension.v1.InputDatasetWithDelegate;
import io.openlineage.shaded.spark.extension.v1.InputDatasetWithIdentifier;
import io.openlineage.shaded.spark.extension.v1.InputLineageNode;
import io.openlineage.shaded.spark.extension.v1.LineageRelation;
import io.openlineage.shaded.spark.extension.v1.LineageRelationProvider;
import io.openlineage.shaded.spark.extension.v1.OutputDatasetWithDelegate;
import io.openlineage.shaded.spark.extension.v1.OutputDatasetWithIdentifier;
import io.openlineage.shaded.spark.extension.v1.OutputLineageNode;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SparkOpenLineageExtensionVisitorWrapperTest {
  static SparkOpenLineageConfig sparkOpenLineageConfig = mock(SparkOpenLineageConfig.class);
  static SparkListenerEvent event = mock(SparkListenerEvent.class);
  static Map<String, String> options = Collections.singletonMap("path", "some-path");
  static SQLContext sqlContext = mock(SQLContext.class);

  SparkOpenLineageExtensionVisitorWrapper wrapper =
      new SparkOpenLineageExtensionVisitorWrapper(sparkOpenLineageConfig);

  @BeforeAll
  static void before() {
    when(sparkOpenLineageConfig.getTestExtensionProvider())
        .thenReturn("io.openlineage.spark.extension.TestOpenLineageExtensionProvider");
  }

  @Test
  void testLineageRelationNode() {
    // given
    LineageRelation lineageRelation =
        (sparkListenerEventName, openLineage) ->
            new DatasetIdentifier("name", "namespace")
                .withSymlink(
                    new DatasetIdentifier.Symlink(
                        "name1", "namespace1", DatasetIdentifier.SymlinkType.TABLE));
    // when
    assertThat(wrapper.isDefinedAt(lineageRelation)).isTrue();
    io.openlineage.client.utils.DatasetIdentifier result =
        wrapper.getLineageDatasetIdentifier(lineageRelation, event.getClass().getName());

    // then
    assertThat(result).extracting("name").isEqualTo("name");
    assertThat(result).extracting("namespace").isEqualTo("namespace");
    assertThat(result)
        .extracting("symlinks")
        .isEqualTo(
            Collections.singletonList(
                new io.openlineage.client.utils.DatasetIdentifier.Symlink(
                    "name1",
                    "namespace1",
                    io.openlineage.client.utils.DatasetIdentifier.SymlinkType.TABLE)));
  }

  @Test
  void testLineageRelationProviderNode() {
    // given
    LineageRelationProvider lineageRelationProvider =
        (sparkListenerEventName, openLineage, sqlContext, parameters) ->
            new DatasetIdentifier("name", "namespace")
                .withSymlink(
                    new DatasetIdentifier.Symlink(
                        "name1", "namespace1", DatasetIdentifier.SymlinkType.TABLE));

    // when
    assertThat(wrapper.isDefinedAt(lineageRelationProvider)).isTrue();
    io.openlineage.client.utils.DatasetIdentifier result =
        wrapper.getLineageDatasetIdentifier(
            lineageRelationProvider, event.getClass().getName(), sqlContext, options);

    // then
    assertThat(result).extracting("name").isEqualTo("name");
    assertThat(result).extracting("namespace").isEqualTo("namespace");
    assertThat(result)
        .extracting("symlinks")
        .isEqualTo(
            Collections.singletonList(
                new io.openlineage.client.utils.DatasetIdentifier.Symlink(
                    "name1",
                    "namespace1",
                    io.openlineage.client.utils.DatasetIdentifier.SymlinkType.TABLE)));
  }

  @Test
  void testInputDatasetWithIdentifierNode() {
    // given
    InputLineageNode inputLineageNode =
        (sparkListenerEventName, ol) ->
            Collections.singletonList(
                new InputDatasetWithIdentifier(
                    new DatasetIdentifier("a", "b"),
                    ol.newDatasetFacetsBuilder()
                        .schema(
                            ol.newSchemaDatasetFacetBuilder()
                                .fields(
                                    Arrays.asList(
                                        ol.newSchemaDatasetFacetFieldsBuilder()
                                            .name("user_id")
                                            .type("int64")
                                            .build()))
                                .build()),
                    ol.newInputDatasetInputFacetsBuilder()
                        .dataQualityMetrics(
                            ol.newDataQualityMetricsInputDatasetFacetBuilder()
                                .rowCount(10L)
                                .bytes(20L)
                                .fileCount(5L)
                                .columnMetrics(
                                    ol.newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder()
                                        .put(
                                            "mycol",
                                            ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder()
                                                .count(10D)
                                                .distinctCount(10L)
                                                .max(30D)
                                                .min(5D)
                                                .nullCount(1L)
                                                .sum(3000D)
                                                .quantiles(
                                                    ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder()
                                                        .put("25", 52D)
                                                        .build())
                                                .build())
                                        .build())
                                .build())));

    // when
    assertThat(wrapper.isDefinedAt(inputLineageNode)).isTrue();
    Pair<List<InputDataset>, List<Object>> inputs =
        wrapper.getInputs(inputLineageNode, event.getClass().getName());

    // then
    assertThat(inputs).isNotNull();
    InputDataset inputDataset = inputs.getLeft().get(0);
    assertThat(inputDataset.getName()).isEqualTo("a");
    assertThat(inputDataset.getNamespace()).isEqualTo("b");
    OpenLineage.SchemaDatasetFacetFields schemaDatasetFacetFields =
        inputDataset.getFacets().getSchema().getFields().get(0);
    assertThat(schemaDatasetFacetFields.getName()).isEqualTo("user_id");
    assertThat(schemaDatasetFacetFields.getType()).isEqualTo("int64");
    OpenLineage.DataQualityMetricsInputDatasetFacet dq =
        inputDataset.getInputFacets().getDataQualityMetrics();
    assertThat(dq.getRowCount()).isEqualTo(10);
    assertThat(dq.getBytes()).isEqualTo(20);
    assertThat(dq.getFileCount()).isEqualTo(5);
    OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetricsAdditional colMetrics =
        dq.getColumnMetrics().getAdditionalProperties().get("mycol");
    assertThat(colMetrics.getCount()).isEqualTo(10);
    assertThat(colMetrics.getDistinctCount()).isEqualTo(10);
    assertThat(colMetrics.getMax()).isEqualTo(30);
    assertThat(colMetrics.getMin()).isEqualTo(5);
    assertThat(colMetrics.getNullCount()).isEqualTo(1);
    assertThat(colMetrics.getSum()).isEqualTo(3000);
    assertThat(colMetrics.getQuantiles().getAdditionalProperties().get("25")).isEqualTo(52);
  }

  @Test
  void testInputDatasetWithDelegate() {
    // given
    LogicalPlan delegate = mock(LogicalPlan.class);
    InputLineageNode inputLineageNode =
        (sparkListenerEventName, ol) ->
            Collections.singletonList(
                new InputDatasetWithDelegate(
                    delegate,
                    ol.newDatasetFacetsBuilder(),
                    ol.newInputDatasetInputFacetsBuilder()));

    // when
    assertThat(wrapper.isDefinedAt(inputLineageNode)).isTrue();
    Pair<List<InputDataset>, List<Object>> inputs =
        wrapper.getInputs(inputLineageNode, event.getClass().getName());

    // then
    assertThat(inputs.getRight()).isNotEmpty().isEqualTo(Collections.singletonList(delegate));
  }

  @Test
  void testOutputDatasetWithIdentifier() {
    // given
    OutputLineageNode outputLineageNode =
        (sparkListenerEventName, ol) ->
            Collections.singletonList(
                new OutputDatasetWithIdentifier(
                    new DatasetIdentifier("a", "b"),
                    ol.newDatasetFacetsBuilder(),
                    ol.newOutputDatasetOutputFacetsBuilder()));

    // when
    Pair<List<OpenLineage.OutputDataset>, List<Object>> outputs =
        wrapper.getOutputs(outputLineageNode, event.getClass().getName());

    // then
    assertThat(outputs.getLeft()).isNotEmpty();
    OpenLineage.OutputDataset outputDataset = outputs.getLeft().get(0);
    assertThat(outputDataset.getName()).isEqualTo("a");
    assertThat(outputDataset.getNamespace()).isEqualTo("b");
  }

  void testOutputDatasetWithDelegate() {
    // given
    LogicalPlan delegate = mock(LogicalPlan.class);
    OutputLineageNode outputLineageNode =
        (sparkListenerEventName, openLineage) ->
            Collections.singletonList(
                new OutputDatasetWithDelegate(
                    delegate,
                    openLineage.newDatasetFacetsBuilder(),
                    openLineage.newOutputDatasetOutputFacetsBuilder()));
    // when
    assertThat(wrapper.isDefinedAt(outputLineageNode)).isTrue();
    Pair<List<InputDataset>, List<Object>> inputs =
        wrapper.getInputs(outputLineageNode, event.getClass().getName());

    // then
    assertThat(inputs.getRight()).isNotEmpty().isEqualTo(Collections.singletonList(delegate));
  }
}
