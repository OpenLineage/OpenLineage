package io.openlineage.spark3.agent.lifecycle.plan.columnLineage.customVisitors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static scala.collection.JavaConverters.collectionAsScalaIterableConverter;

import io.openlineage.spark3.agent.lifecycle.plan.columnLineage.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.columnLineage.ExpressionDependencyCollector;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;
import scala.collection.Seq;

public class IcebergMergeIntoDependencyVisitorTest {

  Attribute output1 = mock(Attribute.class);
  Attribute output2 = mock(Attribute.class);

  ExprId outputExprId1 = mock(ExprId.class);
  ExprId outputExprId2 = mock(ExprId.class);

  Seq<Attribute> outputs =
      scala.collection.JavaConverters.collectionAsScalaIterableConverter(
              Arrays.asList(output1, output2))
          .asScala()
          .toSeq();

  Expression expr1 = mock(Expression.class, withSettings().extraInterfaces(NamedExpression.class));
  Expression expr2 = mock(Expression.class, withSettings().extraInterfaces(NamedExpression.class));
  Expression expr3 = mock(Expression.class, withSettings().extraInterfaces(NamedExpression.class));
  Expression expr4 = mock(Expression.class, withSettings().extraInterfaces(NamedExpression.class));

  Seq<Expression> seq1 =
      collectionAsScalaIterableConverter(Arrays.asList(expr1, expr2)).asScala().toSeq();

  Seq<Expression> seq2 =
      collectionAsScalaIterableConverter(Arrays.asList(expr3, expr4)).asScala().toSeq();

  IcebergMergeIntoDependencyVisitor visitor = new IcebergMergeIntoDependencyVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  public void testCollectMatched() {
    when(output1.exprId()).thenReturn(outputExprId1);
    when(output2.exprId()).thenReturn(outputExprId2);

    try (MockedStatic<ExpressionDependencyCollector> mockStatic =
        mockStatic(ExpressionDependencyCollector.class)) {
      List<Option<Seq<Expression>>> matched = Arrays.asList(Option.apply(seq1), Option.apply(seq2));
      visitor.collect(outputs, matched, Collections.emptyList(), builder);

      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr1, outputExprId1, builder),
          times(1));
      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr2, outputExprId2, builder),
          times(1));
      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr3, outputExprId1, builder),
          times(1));
      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr4, outputExprId2, builder),
          times(1));
    }
  }

  @Test
  public void testCollectNotMatched() {
    when(output1.exprId()).thenReturn(outputExprId1);
    when(output2.exprId()).thenReturn(outputExprId2);

    try (MockedStatic<ExpressionDependencyCollector> mockStatic =
        mockStatic(ExpressionDependencyCollector.class)) {
      List<Option<Seq<Expression>>> notMatched =
          Arrays.asList(Option.apply(seq1), Option.apply(seq2));
      visitor.collect(outputs, Collections.emptyList(), notMatched, builder);

      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr1, outputExprId1, builder),
          times(1));
      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr2, outputExprId2, builder),
          times(1));
      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr3, outputExprId1, builder),
          times(1));
      mockStatic.verify(
          () -> ExpressionDependencyCollector.traverseExpression(expr4, outputExprId2, builder),
          times(1));
    }
  }
}
