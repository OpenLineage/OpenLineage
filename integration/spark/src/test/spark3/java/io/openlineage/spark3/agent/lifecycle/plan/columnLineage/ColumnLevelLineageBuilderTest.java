package io.openlineage.spark3.agent.lifecycle.plan.columnLineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

class ColumnLevelLineageBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
  StructType schema =
      new StructType(
          new StructField[] {
            new StructField("a", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("b", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
          });
  ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);

  ExprId rootExprId = mock(ExprId.class);
  ExprId childExprId = mock(ExprId.class);
  ExprId grandChildExprId1 = mock(ExprId.class);
  ExprId grandChildExprId2 = mock(ExprId.class);

  @BeforeEach
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  public void testEmptyOutput() {
    assertTrue(builder.getInputsUsedFor("non-existing-output").isEmpty());
  }

  @Test
  public void testSingleInputSingleOutput() {
    DatasetIdentifier di = new DatasetIdentifier("t", "db");
    builder.addOutput(rootExprId, "a");
    builder.addInput(rootExprId, di, "inputA");

    List<Pair<DatasetIdentifier, String>> inputs = builder.getInputsUsedFor("a");

    assertTrue(builder.hasOutputs());
    assertEquals(1, inputs.size());
    assertEquals("inputA", inputs.get(0).getRight());
    assertEquals(di, inputs.get(0).getLeft());
  }

  @Test
  public void testMultipleOutputs() {
    DatasetIdentifier di = new DatasetIdentifier("t", "db");
    builder.addOutput(rootExprId, "a");
    builder.addOutput(rootExprId, "b");
    builder.addInput(rootExprId, di, "inputA");

    assertEquals(1, builder.getInputsUsedFor("a").size());
    assertEquals(1, builder.getInputsUsedFor("b").size());
  }

  @Test
  public void testMultipleInputsAndSingleOutputWithNestedExpressions() {
    DatasetIdentifier di1 = new DatasetIdentifier("t1", "db");
    DatasetIdentifier di2 = new DatasetIdentifier("t2", "db");

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
  public void testCycledExpressionDependency() {
    builder.addOutput(rootExprId, "a");
    builder.addDependency(rootExprId, childExprId);
    builder.addDependency(childExprId, rootExprId); // cycle that should not happen

    List<Pair<DatasetIdentifier, String>> inputs = builder.getInputsUsedFor("a");
    assertEquals(0, inputs.size());
  }

  @Test
  public void testBuild() {
    DatasetIdentifier diA = new DatasetIdentifier("tableA", "db");
    DatasetIdentifier diB = new DatasetIdentifier("tableB", "db");

    builder.addOutput(rootExprId, "a");
    builder.addInput(rootExprId, diA, "inputA");
    builder.addInput(rootExprId, diB, "inputB");

    List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> facetFields =
        builder.build().getAdditionalProperties().get("a").getInputFields();

    assertEquals(2, facetFields.size());

    assertEquals("db", facetFields.get(0).getNamespace());
    assertEquals("tableA", facetFields.get(0).getName());
    assertEquals("inputA", facetFields.get(0).getField());

    assertEquals("db", facetFields.get(0).getNamespace());
    assertEquals("tableA", facetFields.get(0).getName());
    assertEquals("inputA", facetFields.get(0).getField());
  }

  @Test
  public void testBuildWithEmptyInputs() {
    builder.addOutput(rootExprId, "a");
    builder.addDependency(rootExprId, childExprId);

    // no inputs
    assertEquals(0, builder.build().getAdditionalProperties().size());
  }

  @Test
  public void testBuildWithDuplicatedInputs() {
    DatasetIdentifier di = new DatasetIdentifier("tableA", "db");

    builder.addOutput(rootExprId, "a");
    builder.addInput(rootExprId, di, "inputA");
    builder.addInput(childExprId, di, "inputA"); // the same input with different exprId
    builder.addDependency(rootExprId, childExprId);

    List<OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFields> facetFields =
        builder.build().getAdditionalProperties().get("a").getInputFields();

    assertEquals(1, facetFields.size());
  }
}
