/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ColumnLevelLineageBuilderTest {

  private static final String TABLE_A = "tableA";
  private static final String INPUT_A = "inputA";
  private static final String DB = "db";
  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  OpenLineage.SchemaDatasetFacet schema =
      openLineage.newSchemaDatasetFacet(
          Arrays.asList(
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("a").type("int").build(),
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("b").type("int").build()));
  ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);

  ExprId rootExprId = mock(ExprId.class);
  ExprId childExprId = mock(ExprId.class);
  ExprId grandChildExprId1 = mock(ExprId.class);
  ExprId grandChildExprId2 = mock(ExprId.class);

  @BeforeEach
  void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  void testEmptyOutput() {
    assertTrue(builder.getInputsUsedFor("non-existing-output").isEmpty());
  }

  @Test
  void testSingleInputSingleOutput() {
    DatasetIdentifier di = new DatasetIdentifier("t", DB);
    builder.addOutput(rootExprId, "a");
    builder.addInput(rootExprId, di, INPUT_A);

    List<Pair<DatasetIdentifier, String>> inputs = builder.getInputsUsedFor("a");

    assertTrue(builder.hasOutputs());
    assertEquals(1, inputs.size());
    assertEquals(INPUT_A, inputs.get(0).getRight());
    assertEquals(di, inputs.get(0).getLeft());
  }

  @Test
  void testMultipleOutputs() {
    DatasetIdentifier di = new DatasetIdentifier("t", DB);
    builder.addOutput(rootExprId, "a");
    builder.addOutput(rootExprId, "b");
    builder.addInput(rootExprId, di, INPUT_A);

    assertEquals(1, builder.getInputsUsedFor("a").size());
    assertEquals(1, builder.getInputsUsedFor("b").size());
  }

  @Test
  void testMultipleInputsAndSingleOutputWithNestedExpressions() {
    DatasetIdentifier di1 = new DatasetIdentifier("t1", DB);
    DatasetIdentifier di2 = new DatasetIdentifier("t2", DB);

    builder.addOutput(rootExprId, "a");
    builder.addDependency(rootExprId, childExprId);
    builder.addDependency(childExprId, grandChildExprId1);
    builder.addDependency(childExprId, grandChildExprId2);
    builder.addInput(grandChildExprId1, di1, "input1");
    builder.addInput(grandChildExprId1, di2, "input2");

    List<Pair<DatasetIdentifier, String>> inputs = builder.getInputsUsedFor("a");

    assertTrue(builder.hasOutputs());
    assertEquals(2, inputs.size());
    assertEquals("input1", inputs.get(0).getRight());
    assertEquals("input2", inputs.get(1).getRight());
    assertEquals(di1, inputs.get(0).getLeft());
    assertEquals(di2, inputs.get(1).getLeft());
  }

  @Test
  void testCycledExpressionDependency() {
    builder.addOutput(rootExprId, "a");
    builder.addDependency(rootExprId, childExprId);
    builder.addDependency(childExprId, rootExprId); // cycle that should not happen

    List<Pair<DatasetIdentifier, String>> inputs = builder.getInputsUsedFor("a");
    assertEquals(0, inputs.size());
  }

  @Test
  void testBuild() {
    DatasetIdentifier diA = new DatasetIdentifier(TABLE_A, DB);
    DatasetIdentifier diB = new DatasetIdentifier("tableB", DB);

    builder.addOutput(rootExprId, "a");
    builder.addInput(rootExprId, diA, INPUT_A);
    builder.addInput(rootExprId, diB, "inputB");

    List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> facetFields =
        builder.build().getAdditionalProperties().get("a").getInputFields();

    assertEquals(2, facetFields.size());

    assertEquals(DB, facetFields.get(0).getNamespace());
    assertEquals(TABLE_A, facetFields.get(0).getName());
    assertEquals(INPUT_A, facetFields.get(0).getField());

    assertEquals(DB, facetFields.get(0).getNamespace());
    assertEquals(TABLE_A, facetFields.get(0).getName());
    assertEquals(INPUT_A, facetFields.get(0).getField());
  }

  @Test
  void testBuildWithEmptyInputs() {
    builder.addOutput(rootExprId, "a");
    builder.addDependency(rootExprId, childExprId);

    // no inputs
    assertEquals(0, builder.build().getAdditionalProperties().size());
  }

  @Test
  void testBuildWithDuplicatedInputs() {
    DatasetIdentifier di = new DatasetIdentifier(TABLE_A, DB);

    builder.addOutput(rootExprId, "a");
    builder.addInput(rootExprId, di, INPUT_A);
    builder.addInput(childExprId, di, INPUT_A); // the same input with different exprId
    builder.addDependency(rootExprId, childExprId);

    List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> facetFields =
        builder.build().getAdditionalProperties().get("a").getInputFields();

    assertEquals(1, facetFields.size());
  }
}
