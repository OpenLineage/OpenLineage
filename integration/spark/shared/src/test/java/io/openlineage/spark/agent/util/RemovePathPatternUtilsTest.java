/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.spark.agent.util.RemovePathPatternUtils.SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetInputFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacets;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RemovePathPatternUtilsTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  SparkConf conf;
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  @BeforeEach
  public void setup() {
    conf = mock(SparkConf.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(context.getSparkContext()).thenReturn(sparkContext);
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(sparkContext.conf()).thenReturn(conf);
  }

  @Test
  void testRemoveOutputsPathPatternWhenNotConfigured() {
    when(conf.contains("spark.openlineage.dataset.removePath.pattern")).thenReturn(false);
    List<OutputDataset> outputs = mock(List.class);
    assertThat(RemovePathPatternUtils.removeOutputsPathPattern(context, outputs))
        .isEqualTo(outputs);
  }

  @Test
  void testRemoveInputsPathPatternWhenNotConfigured() {
    when(conf.contains("spark.openlineage.dataset.removePath.pattern")).thenReturn(false);
    List<InputDataset> inputs = mock(List.class);
    assertThat(RemovePathPatternUtils.removeInputsPathPattern(context, inputs)).isEqualTo(inputs);
  }

  @Test
  void testInvalidGroupInPattern() {
    when(conf.contains("spark.openlineage.dataset.removePath.pattern")).thenReturn(true);
    when(conf.get("spark.openlineage.dataset.removePath.pattern"))
        .thenReturn("(.*)(?<nonValidGroup>\\/.*\\/.*\\/.*)");

    List<InputDataset> inputs =
        Arrays.asList(
            openLineage.newInputDatasetBuilder().name("d1").build(),
            openLineage.newInputDatasetBuilder().name("d2").build());
    assertThat(RemovePathPatternUtils.removeInputsPathPattern(context, inputs)).isEqualTo(inputs);
  }

  @Test
  void testDatasetNameRemovesPatternForInputs() {
    when(conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN)).thenReturn(true);
    when(conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .thenReturn("(.*)(?<remove>\\/.*\\/.*\\/.*)");

    InputDatasetInputFacets inputFacets = mock(InputDatasetInputFacets.class);
    DatasetFacets datasetFacets = mock(DatasetFacets.class);
    InputDataset inputDataset =
        openLineage.newInputDatasetBuilder().name("some-other-name").build();

    List<InputDataset> inputs =
        Arrays.asList(
            openLineage
                .newInputDatasetBuilder()
                .name("/my-whatever-path/year=2023/month=04/day=24")
                .namespace("ns")
                .inputFacets(inputFacets)
                .facets(datasetFacets)
                .build(),
            inputDataset);

    List<InputDataset> modifiedInputs =
        RemovePathPatternUtils.removeInputsPathPattern(context, inputs);

    assertThat(modifiedInputs).hasSize(2);
    assertThat(modifiedInputs.get(1)).isEqualTo(inputDataset);
    assertThat(modifiedInputs.get(0))
        .hasFieldOrPropertyWithValue("name", "/my-whatever-path")
        .hasFieldOrPropertyWithValue("namespace", "ns")
        .hasFieldOrPropertyWithValue("inputFacets", inputFacets)
        .hasFieldOrPropertyWithValue("facets", datasetFacets);
  }

  @Test
  void testDatasetNameRemovesPatternForOutputs() {
    when(conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN)).thenReturn(true);
    when(conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .thenReturn("(.*)(?<remove>\\/.*\\/.*\\/.*)");

    OutputDatasetOutputFacets outputFacets = mock(OutputDatasetOutputFacets.class);
    DatasetFacets datasetFacets = mock(DatasetFacets.class);
    OutputDataset outputDataset =
        openLineage.newOutputDatasetBuilder().name("some-other-name").build();

    List<OutputDataset> outputs =
        Arrays.asList(
            openLineage
                .newOutputDatasetBuilder()
                .name("/my-whatever-path/year=2023/month=04/day=24")
                .namespace("ns")
                .outputFacets(outputFacets)
                .facets(datasetFacets)
                .build(),
            outputDataset);

    List<OutputDataset> modifiedOutputs =
        RemovePathPatternUtils.removeOutputsPathPattern(context, outputs);

    assertThat(modifiedOutputs).hasSize(2);
    assertThat(modifiedOutputs.get(1)).isEqualTo(outputDataset);
    assertThat(modifiedOutputs.get(0))
        .hasFieldOrPropertyWithValue("name", "/my-whatever-path")
        .hasFieldOrPropertyWithValue("namespace", "ns")
        .hasFieldOrPropertyWithValue("outputFacets", outputFacets)
        .hasFieldOrPropertyWithValue("facets", datasetFacets);
  }
}
