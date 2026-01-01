/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class RemovePathPatternUtilsTest {

  private static final String TEST_PATH_CUSTOMERS = "/data/customers";
  private static final String TEST_PATH_ORDERS = "/data/orders";
  private static final String TEST_PATH_SOURCE = "/data/source";
  private static final String TEST_PATH_OUTPUT = "/output/data";
  private static final String TEST_PATH_SUFFIX = "/year=2023/month=04/day=24";

  OpenLineageContext context = mock(OpenLineageContext.class);
  SparkConf conf;
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  @BeforeEach
  public void setup() {
    conf = mock(SparkConf.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
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

  @Test
  void testRemoveOutputsPathPatternWithColumnLineageFacet() {
    when(conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN)).thenReturn(true);
    when(conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .thenReturn("(.*)(?<remove>\\/.*\\/.*\\/.*)");

    OutputDataset outputDataset =
        buildSampleOutputDatasetWithColumnLineage(
            TEST_PATH_OUTPUT + TEST_PATH_SUFFIX,
            true, // includeDatasetInputFields
            true // usePathsWithPatterns
            );

    List<OutputDataset> outputs = Arrays.asList(outputDataset);
    List<OutputDataset> modifiedOutputs =
        RemovePathPatternUtils.removeOutputsPathPattern(context, outputs);

    assertThat(modifiedOutputs).hasSize(1);
    OutputDataset result = modifiedOutputs.get(0);

    // Verify dataset name pattern removal
    assertThat(result.getName()).isEqualTo(TEST_PATH_OUTPUT);

    // Verify column lineage facet processing
    OpenLineage.ColumnLineageDatasetFacet resultFacet = result.getFacets().getColumnLineage();

    // Verify dataset-level InputField pattern removal
    assertThat(resultFacet.getDataset()).hasSize(1);
    assertThat(resultFacet.getDataset().get(0).getName()).isEqualTo(TEST_PATH_SOURCE);

    // Verify field-level InputField pattern removal
    OpenLineage.ColumnLineageDatasetFacetFieldsAdditional resultFieldAdditional =
        resultFacet.getFields().getAdditionalProperties().get("output_field");
    assertThat(resultFieldAdditional.getInputFields()).hasSize(2);
    assertThat(resultFieldAdditional.getInputFields().get(0).getName())
        .isEqualTo(TEST_PATH_CUSTOMERS);
    assertThat(resultFieldAdditional.getInputFields().get(1).getName()).isEqualTo(TEST_PATH_ORDERS);
  }

  @Test
  void testRemoveOutputsPathPatternWithOnlyFieldMappingsInColumnLineageFacet() {
    when(conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN)).thenReturn(true);
    when(conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .thenReturn("(.*)(?<remove>\\/.*\\/.*\\/.*)");

    OutputDataset outputDataset =
        buildSampleOutputDatasetWithColumnLineage(
            TEST_PATH_OUTPUT + TEST_PATH_SUFFIX,
            false, // includeDatasetInputFields
            true // usePathsWithPatterns
            );

    List<OutputDataset> outputs = Arrays.asList(outputDataset);
    List<OutputDataset> modifiedOutputs =
        RemovePathPatternUtils.removeOutputsPathPattern(context, outputs);

    assertThat(modifiedOutputs).hasSize(1);
    OutputDataset result = modifiedOutputs.get(0);

    // Verify dataset name pattern removal
    assertThat(result.getName()).isEqualTo(TEST_PATH_OUTPUT);

    // Verify only field-level InputField pattern removal
    OpenLineage.ColumnLineageDatasetFacet resultFacet = result.getFacets().getColumnLineage();
    assertThat(resultFacet.getDataset()).isEmpty();

    OpenLineage.ColumnLineageDatasetFacetFieldsAdditional resultFieldAdditional =
        resultFacet.getFields().getAdditionalProperties().get("output_field");
    assertThat(resultFieldAdditional.getInputFields()).hasSize(2);
    assertThat(resultFieldAdditional.getInputFields().get(0).getName())
        .isEqualTo(TEST_PATH_CUSTOMERS);
    assertThat(resultFieldAdditional.getInputFields().get(1).getName()).isEqualTo(TEST_PATH_ORDERS);
  }

  @Test
  void testRemoveOutputsPathPatternWithColumnLineageNoPatternMatch() {
    when(conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN)).thenReturn(true);
    when(conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .thenReturn("(.*)(?<remove>\\/.*\\/.*\\/.*)");

    OutputDataset outputDataset =
        buildSampleOutputDatasetWithColumnLineage(
            TEST_PATH_OUTPUT,
            true, // includeDatasetInputFields
            false // no path patterns - won't match
            );

    List<OutputDataset> outputs = Arrays.asList(outputDataset);
    List<OutputDataset> modifiedOutputs =
        RemovePathPatternUtils.removeOutputsPathPattern(context, outputs);

    assertThat(modifiedOutputs).hasSize(1);
    OutputDataset result = modifiedOutputs.get(0);

    // Verify dataset name unchanged (no pattern match)
    assertThat(result.getName()).isEqualTo(TEST_PATH_OUTPUT);

    // Verify column lineage facet unchanged (no pattern match)
    OpenLineage.ColumnLineageDatasetFacet resultFacet = result.getFacets().getColumnLineage();

    assertThat(resultFacet.getDataset()).hasSize(1);
    assertThat(resultFacet.getDataset().get(0).getName()).isEqualTo(TEST_PATH_SOURCE);

    OpenLineage.ColumnLineageDatasetFacetFieldsAdditional resultFieldAdditional =
        resultFacet.getFields().getAdditionalProperties().get("output_field");
    assertThat(resultFieldAdditional.getInputFields()).hasSize(2);
    assertThat(resultFieldAdditional.getInputFields().get(0).getName())
        .isEqualTo(TEST_PATH_CUSTOMERS);
    assertThat(resultFieldAdditional.getInputFields().get(1).getName()).isEqualTo(TEST_PATH_ORDERS);
  }

  private OpenLineage.ColumnLineageDatasetFacet buildSampleColumnLineageFacet(
      boolean includeDatasetInputFields, boolean usePathsWithPatterns) {

    String pathSuffix = usePathsWithPatterns ? TEST_PATH_SUFFIX : "";

    // Build field mappings
    OpenLineage.ColumnLineageDatasetFacetFields fields;
    OpenLineage.InputField inputField1 =
        openLineage
            .newInputFieldBuilder()
            .namespace("ns")
            .name(TEST_PATH_CUSTOMERS + pathSuffix)
            .field("customer_id")
            .build();
    OpenLineage.InputField inputField2 =
        openLineage
            .newInputFieldBuilder()
            .namespace("ns")
            .name(TEST_PATH_ORDERS + pathSuffix)
            .field("order_id")
            .build();
    OpenLineage.ColumnLineageDatasetFacetFieldsAdditional fieldAdditional =
        openLineage.newColumnLineageDatasetFacetFieldsAdditional(
            Arrays.asList(inputField1, inputField2), "Direct mapping", "IDENTITY");
    fields =
        openLineage
            .newColumnLineageDatasetFacetFieldsBuilder()
            .put("output_field", fieldAdditional)
            .build();

    // Build dataset input fields
    List<OpenLineage.InputField> datasetInputFields = new ArrayList();
    if (includeDatasetInputFields) {
      OpenLineage.InputField datasetInputField =
          openLineage
              .newInputFieldBuilder()
              .namespace("ns")
              .name(TEST_PATH_SOURCE + pathSuffix)
              .field("source_field")
              .build();
      datasetInputFields = Arrays.asList(datasetInputField);
    }

    return openLineage.newColumnLineageDatasetFacet(fields, datasetInputFields);
  }

  private OutputDataset buildSampleOutputDatasetWithColumnLineage(
      String datasetName, boolean includeDatasetInputFields, boolean usePathsWithPatterns) {

    OpenLineage.ColumnLineageDatasetFacet columnLineageFacet =
        buildSampleColumnLineageFacet(includeDatasetInputFields, usePathsWithPatterns);
    OpenLineage.DatasetFacets datasetFacets =
        openLineage.newDatasetFacetsBuilder().columnLineage(columnLineageFacet).build();

    return openLineage
        .newOutputDatasetBuilder()
        .name(datasetName)
        .namespace("ns")
        .facets(datasetFacets)
        .build();
  }
}
