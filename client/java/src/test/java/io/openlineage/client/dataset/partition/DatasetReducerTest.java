/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputSubsetInputDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetConfig;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DatasetReducerTest {

  OpenLineage openLineage;
  DatasetReducer reducer;
  List<InputDataset> inputs;
  List<OutputDataset> outputs;

  @BeforeEach
  public void setup() {
    openLineage = new OpenLineage(URI.create("http://test.producer"));

    inputs = new ArrayList<>();
    inputs.add(inputFactory("/tmp/some/path1"));
    inputs.add(inputFactory("/tmp/some/path2"));
    inputs.add(inputFactory("/tmp/some/path3"));

    outputs = new ArrayList<>();
    outputs.add(outputFactory("/tmp/some/path1"));
    outputs.add(outputFactory("/tmp/some/path2"));
    outputs.add(outputFactory("/tmp/some/path3"));

    reducer = new DatasetReducer(openLineage, new DatasetConfig());
  }

  @Test
  void testConfigIsNull() {
    // merging should be applied, test makes sure no NPE is thrown
    reducer = new DatasetReducer(openLineage, null);

    assertThat(
            reducer.reduceInputs(inputs).stream()
                .map(InputDataset::getName)
                .collect(Collectors.toList()))
        .contains("/tmp/some/path1", "/tmp/some/path2", "/tmp/some/path3");
    assertThat(
            reducer.reduceOutputs(outputs).stream()
                .map(OutputDataset::getName)
                .collect(Collectors.toList()))
        .contains("/tmp/some/path1", "/tmp/some/path2", "/tmp/some/path3");
  }

  @Test
  void testTrimLooseDateDetected() {
    inputs.add(inputFactory("/tmp/some/tested-path/20250721/20250722T901Z/key=value"));
    inputs.add(inputFactory("/tmp/some/tested-path/20250720/20250721T901Z/key=value"));

    List<InputDataset> reduceInputs = reducer.reduceInputs(inputs);
    assertThat(reduceInputs).hasSize(4);

    Optional<InputDataset> input =
        reducer.reduceInputs(inputs).stream()
            .filter(i -> i.getName().endsWith("tested-path"))
            .findFirst();
    assertThat(input).get().extracting("name").isEqualTo("/tmp/some/tested-path");

    InputSubsetInputDatasetFacet subset = input.get().getInputFacets().getSubset();
    assertThat(subset).isNotNull();
    assertThat(subset.getInputCondition()).isNotNull();
    assertThat(subset.getInputCondition().getLocations()).isNotNull();
    assertThat(subset.getInputCondition().getLocations())
        .contains(
            "/tmp/some/tested-path/20250721/20250722T901Z/key=value",
            "/tmp/some/tested-path/20250720/20250721T901Z/key=value");
  }

  @Test
  void testTrimAtKeyValueDirs() {
    inputs.clear();
    inputs.add(inputFactory("/tmp/some/tested-path/date=20250721/20250722T901Z"));
    inputs.add(inputFactory("/tmp/some/tested-path/date=20250720/20250721T901Z"));
    inputs.add(inputFactory("/tmp/some/tested-path/date=20250722/20250721T901Z"));
    inputs.add(inputFactory("/tmp/some/path4"));

    List<InputDataset> reduceInputs = reducer.reduceInputs(inputs);
    assertThat(reduceInputs).hasSize(2);
    assertThat(reduceInputs.get(0).getName()).isEqualTo("/tmp/some/tested-path");

    InputSubsetInputDatasetFacet subset = reduceInputs.get(0).getInputFacets().getSubset();
    assertThat(subset).isNotNull();
    assertThat(subset.getInputCondition()).isNotNull();
    assertThat(subset.getInputCondition().getLocations()).isNotNull();
    assertThat(subset.getInputCondition().getLocations())
        .contains(
            "/tmp/some/tested-path/date=20250721/20250722T901Z",
            "/tmp/some/tested-path/date=20250720/20250721T901Z",
            "/tmp/some/tested-path/date=20250722/20250721T901Z");
  }

  /** Datasets should not be reduced if they have different facets */
  @Test
  void testFacetsAreDifferent() {
    inputs = new ArrayList<>();
    inputs.add(inputFactory("/tmp/some/tested-path/20250721/20250722T901Z"));

    // different dataset facet
    inputs.add(
        openLineage
            .newInputDatasetBuilder()
            .name("/tmp/some/tested-path/20250722/20250722T901Z")
            .namespace("namespace")
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .documentation(
                        openLineage.newDocumentationDatasetFacet(
                            "description", "description")) // different facet added
                    .schema(openLineage.newSchemaDatasetFacetBuilder().build())
                    .build())
            .inputFacets(openLineage.newInputDatasetInputFacetsBuilder().build())
            .build());

    // should not be reduced
    List<InputDataset> reduceInputs = reducer.reduceInputs(inputs);
    assertThat(reduceInputs).hasSize(2);
  }

  /** Datasets should not be reduced if they have different facets */
  @Test
  void testInputFacetsAreDifferent() {
    inputs = new ArrayList<>();
    inputs.add(inputFactory("/tmp/some/tested-path/20250721/20250722T901Z"));
    // different input dataset facet
    inputs.add(
        openLineage
            .newInputDatasetBuilder()
            .name("/tmp/some/tested-path/20250723/20250722T901Z")
            .namespace("namespace")
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .schema(openLineage.newSchemaDatasetFacetBuilder().build())
                    .build())
            .inputFacets(
                openLineage
                    .newInputDatasetInputFacetsBuilder()
                    .inputStatistics(
                        openLineage.newInputStatisticsInputDatasetFacetBuilder().build())
                    .build())
            .build());

    // should not be reduced
    List<InputDataset> reduceInputs = reducer.reduceInputs(inputs);
    assertThat(reduceInputs).hasSize(2);
  }

  @Test
  void testSomeFacetsAreDifferent() {
    inputs = new ArrayList<>();
    inputs.add(inputFactory("/tmp/some/tested-path/20250721/20250722T901Z"));
    // different dataset facet
    inputs.add(
        openLineage
            .newInputDatasetBuilder()
            .name("/tmp/some/tested-path/20250722/20250722T901Z")
            .namespace("namespace")
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .documentation(
                        openLineage.newDocumentationDatasetFacet(
                            "description", "description")) // different facet added
                    .schema(openLineage.newSchemaDatasetFacetBuilder().build())
                    .build())
            .inputFacets(openLineage.newInputDatasetInputFacetsBuilder().build())
            .build());
    inputs.add(inputFactory("/tmp/some/tested-path/20250723/20250722T901Z"));

    List<InputDataset> reduceInputs = reducer.reduceInputs(inputs);
    assertThat(reduceInputs).hasSize(2);
    assertThat(true).isTrue();
  }

  @Test
  void testSubsetFacetIsAddedOnlyToReducedDatasets() {
    inputs.add(inputFactory("/tmp/some/tested-path"));

    outputs.clear();
    outputs.add(outputFactory("/tmp/some/tested-path"));

    Optional<InputDataset> input =
        reducer.reduceInputs(inputs).stream()
            .filter(i -> i.getName().endsWith("tested-path"))
            .findFirst();
    assertThat(input).isPresent();
    assertThat(input.get().getName()).isEqualTo("/tmp/some/tested-path");
    assertThat(input.get().getInputFacets().getSubset()).isNull();

    OutputDataset output = reducer.reduceOutputs(outputs).get(outputs.size() - 1);
    assertThat(output.getName()).isEqualTo("/tmp/some/tested-path");
    assertThat(output.getOutputFacets().getAdditionalProperties()).doesNotContainKey("subset");
  }

  @Test
  void testTrimsSingleDirectory() {
    inputs.clear();
    inputs.add(inputFactory("/tmp/some/tested-path/20250721/20250722T901Z/key=value"));

    outputs.clear();
    outputs.add(outputFactory("/tmp/some/tested-path/20250721/20250722T901Z/key=value"));

    List<InputDataset> reduceInputs = reducer.reduceInputs(inputs);
    assertThat(reduceInputs).hasSize(1);
    assertThat(reduceInputs.get(0))
        .extracting(InputDataset::getName)
        .isEqualTo("/tmp/some/tested-path");
    assertThat(reduceInputs.get(0).getInputFacets().getSubset()).isNotNull();

    List<OutputDataset> reduceOutputs = reducer.reduceOutputs(outputs);
    assertThat(reduceOutputs.get(0))
        .extracting(OutputDataset::getName)
        .isEqualTo("/tmp/some/tested-path");
    assertThat(reduceOutputs.get(0).getOutputFacets().getAdditionalProperties())
        .containsKeys("subset");
  }

  OutputDataset outputFactory(String name) {
    return openLineage
        .newOutputDatasetBuilder()
        .name(name)
        .namespace("namespace")
        .facets(
            openLineage
                .newDatasetFacetsBuilder()
                .schema(openLineage.newSchemaDatasetFacetBuilder().build())
                .build())
        .outputFacets(openLineage.newOutputDatasetOutputFacetsBuilder().build())
        .build();
  }

  InputDataset inputFactory(String name) {
    return openLineage
        .newInputDatasetBuilder()
        .name(name)
        .namespace("namespace")
        .facets(
            openLineage
                .newDatasetFacetsBuilder()
                .schema(openLineage.newSchemaDatasetFacetBuilder().build())
                .build())
        .inputFacets(openLineage.newInputDatasetInputFacetsBuilder().build())
        .build();
  }

  @ParameterizedTest
  @ValueSource(strings = {"some_table_20250923", "20250722T0901Z", "202504"})
  void testDatasetsWhichShouldNotBeTrimmed(String datasetName) {
    inputs.clear();
    inputs.add(inputFactory(datasetName));

    List<String> names =
        reducer.reduceInputs(inputs).stream()
            .map(InputDataset::getName)
            .collect(Collectors.toList());

    assertThat(names).containsExactly(datasetName);
  }
}
