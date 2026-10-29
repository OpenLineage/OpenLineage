/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies capturing column lineage of a checkpointed plan with {@link
 * ColumnLevelLineageBuilder#buildCheckpointColumnLineage()} and attaching it to a plan reading the
 * checkpoint.
 */
class CheckpointColumnLineageTest {

  private static final String NAMESPACE = "db";
  private static final DatasetIdentifier TABLE = new DatasetIdentifier("t", NAMESPACE);

  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  OpenLineage.SchemaDatasetFacet schema =
      openLineage.newSchemaDatasetFacet(
          Arrays.asList(
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("a").type("string").build(),
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("b").type("string").build()));

  @BeforeEach
  void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getColumnLineageConfig().setDatasetLineageEnabled(true);
    when(context.getOpenLineageConfig()).thenReturn(config);
  }

  /**
   * Captures lineage of {@code SELECT upper(t.b) AS a, t.b AS b FROM t WHERE t.c = 'x'}, i.e. a
   * field-level dependency with a transformation and a dataset-level dependency on a column which
   * isn't part of the output.
   */
  private CheckpointColumnLineage captureCheckpoint() {
    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);
    ExprId outputA = mock(ExprId.class);
    ExprId outputB = mock(ExprId.class);
    ExprId inputB = mock(ExprId.class);
    ExprId inputC = mock(ExprId.class);

    builder.addOutput(outputA, "a");
    builder.addOutput(outputB, "b");
    builder.addDependency(outputA, inputB, "upper(b)", TransformationInfo.transformation());
    builder.addDependency(outputB, inputB, "b");
    builder.addInput(inputB, TABLE, "b");
    builder.addInput(inputC, TABLE, "c");
    builder.addDatasetDependency(inputC, "c = 'x'");

    return builder.buildCheckpointColumnLineage();
  }

  @Test
  void testCapturedLineageIsKeptSeparatelyPerFieldAndForDataset() {
    CheckpointColumnLineage lineage = captureCheckpoint();

    assertThat(lineage.getFieldInputs("a"))
        .singleElement()
        .satisfies(
            input -> {
              assertThat(input.getInput()).isEqualTo(new Input(TABLE, "b"));
              assertThat(input.getTransformationInfo().getSubType())
                  .isEqualTo(TransformationInfo.Subtypes.TRANSFORMATION);
            });
    assertThat(lineage.getFieldInputs("b"))
        .singleElement()
        .satisfies(input -> assertThat(input.getInput()).isEqualTo(new Input(TABLE, "b")));
    assertThat(lineage.getDatasetDependencyInputs())
        .as("dataset-level dependencies shouldn't be merged into the fields")
        .singleElement()
        .satisfies(input -> assertThat(input.getInput()).isEqualTo(new Input(TABLE, "c")));
  }

  @Test
  void testCheckpointInputIsResolvedAndMergedWithDownstreamTransformations() {
    CheckpointColumnLineage lineage = captureCheckpoint();

    // SELECT coalesce(a, '') AS a, length(b) AS b FROM checkpointed
    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);
    ExprId outputA = mock(ExprId.class);
    ExprId outputB = mock(ExprId.class);
    ExprId checkpointA = mock(ExprId.class);
    ExprId checkpointB = mock(ExprId.class);
    builder.addOutput(outputA, "a");
    builder.addOutput(outputB, "b");
    builder.addDependency(outputA, checkpointA, "coalesce(a, '')");
    builder.addDependency(outputB, checkpointB, "length(b)", TransformationInfo.transformation());
    builder.addCheckpointInput(checkpointA, "a", lineage);
    builder.addCheckpointInput(checkpointB, "b", lineage);
    builder.addCheckpointDatasetDependencies(lineage);

    assertThat(builder.getInputsUsedFor("a"))
        .as("transformation captured within the checkpoint should be kept")
        .singleElement()
        .satisfies(
            input -> {
              assertThat(input.getInput()).isEqualTo(new Input(TABLE, "b"));
              assertThat(input.getTransformationInfo().getSubType())
                  .isEqualTo(TransformationInfo.Subtypes.TRANSFORMATION);
            });
    assertThat(builder.getInputsUsedFor("b"))
        .as("transformation applied downstream should be merged in")
        .singleElement()
        .satisfies(
            input ->
                assertThat(input.getTransformationInfo().getSubType())
                    .isEqualTo(TransformationInfo.Subtypes.TRANSFORMATION));
    assertThat(builder.buildDatasetDependencies(true))
        .hasValueSatisfying(
            fields ->
                assertThat(fields)
                    .singleElement()
                    .satisfies(field -> assertThat(field.getField()).isEqualTo("c")));
  }

  @Test
  void testCheckpointFieldNotReadDownstreamIsIgnored() {
    CheckpointColumnLineage lineage = captureCheckpoint();

    // SELECT a, 'const' AS b FROM checkpointed
    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);
    ExprId checkpointA = mock(ExprId.class);
    ExprId checkpointB = mock(ExprId.class);
    builder.addOutput(checkpointA, "a");
    builder.addOutput(mock(ExprId.class), "b");
    builder.addCheckpointInput(checkpointA, "a", lineage);
    builder.addCheckpointInput(checkpointB, "b", lineage);

    assertThat(builder.getInputsUsedFor("a")).hasSize(1);
    assertThat(builder.getInputsUsedFor("b")).isEmpty();
  }

  /**
   * A checkpoint of a plan reading another checkpoint is captured fully resolved, so it doesn't
   * depend on the lineage of the earlier checkpoint being kept around.
   */
  @Test
  void testChainedCheckpointIsCapturedFullyResolved() {
    CheckpointColumnLineage first = captureCheckpoint();

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);
    ExprId checkpointA = mock(ExprId.class);
    builder.addOutput(checkpointA, "a");
    builder.addCheckpointInput(checkpointA, "a", first);
    builder.addCheckpointDatasetDependencies(first);
    CheckpointColumnLineage second = builder.buildCheckpointColumnLineage();

    List<TransformedInput> inputs = second.getFieldInputs("a");
    assertThat(inputs)
        .singleElement()
        .satisfies(input -> assertThat(input.getInput()).isEqualTo(new Input(TABLE, "b")));
    assertThat(second.getDatasetDependencyInputs())
        .singleElement()
        .satisfies(input -> assertThat(input.getInput()).isEqualTo(new Input(TABLE, "c")));
  }
}
