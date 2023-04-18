package io.openlineage.spark3.agent.lifecycle.plan;

import static io.openlineage.spark3.agent.lifecycle.plan.column.CustomCollectorsUtilsTest.INPUT_COL_NAME;
import static io.openlineage.spark3.agent.lifecycle.plan.column.CustomCollectorsUtilsTest.OUTPUT_COL_NAME;
import static io.openlineage.spark3.agent.lifecycle.plan.column.CustomCollectorsUtilsTest.child;
import static org.mockito.Mockito.mock;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.CustomCollectorsUtilsTest;
import io.openlineage.spark3.agent.lifecycle.plan.column.CustomColumnLineageVisitor;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class CustomColumnLineageVisitorTestImpl implements CustomColumnLineageVisitor {

  @Override
  public void collectInputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node.equals(child)) {
      builder.addInput(
          CustomCollectorsUtilsTest.childExprId, mock(DatasetIdentifier.class), INPUT_COL_NAME);
    }
  }

  @Override
  public void collectOutputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node.equals(child)) {
      builder.addOutput(CustomCollectorsUtilsTest.parentExprId, OUTPUT_COL_NAME);
    }
  }

  @Override
  public void collectExpressionDependencies(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node.equals(child)) {
      builder.addDependency(
          CustomCollectorsUtilsTest.parentExprId, CustomCollectorsUtilsTest.childExprId);
    }
  }
}
