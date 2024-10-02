/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ColumnLevelLineageBuilderOnlyFieldDependenciesTest {

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

  @BeforeEach
  void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
  }

  @Test
  void testBuildFieldsWithEmptyInputs() {
    builder.addOutput(rootExprId, "a");
    builder.addDependency(rootExprId, childExprId);

    // no inputs
    assertEquals(0, builder.buildFields(false).getAdditionalProperties().size());
  }

  @Test
  void testBuildFieldsWithDuplicatedInputs() {
    DatasetIdentifier di = new DatasetIdentifier(TABLE_A, DB);

    builder.addOutput(rootExprId, "a");
    builder.addInput(rootExprId, di, INPUT_A);
    builder.addInput(childExprId, di, INPUT_A); // the same input with different exprId
    builder.addDependency(rootExprId, childExprId);

    List<OpenLineage.InputField> facetFields =
        builder.buildFields(false).getAdditionalProperties().get("a").getInputFields();

    assertEquals(1, facetFields.size());
  }
}
