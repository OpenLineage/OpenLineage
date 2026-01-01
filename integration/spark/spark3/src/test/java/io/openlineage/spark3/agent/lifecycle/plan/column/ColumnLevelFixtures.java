/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.NullOrdering;
import org.apache.spark.sql.catalyst.expressions.SortDirection;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;
import scala.Option;
import scala.collection.immutable.Seq;

public class ColumnLevelFixtures {
  public static final String NAME_1 = "name1";
  public static final String NAME_2 = "name2";
  public static final String NAME_3 = "name3";

  public static final ExprId EXPR_ID_1 = ExprId.apply(21);
  public static final ExprId EXPR_ID_2 = ExprId.apply(22);
  public static final ExprId EXPR_ID_3 = ExprId.apply(23);
  public static final ExprId EXPR_ID_4 = ExprId.apply(24);

  public static AttributeReference field(String name, ExprId exprId) {
    return new AttributeReference(
        name,
        IntegerType$.MODULE$,
        false,
        Metadata$.MODULE$.empty(),
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  public static Literal intLiteral(int value) {
    return new Literal(value, IntegerType$.MODULE$);
  }

  public static EqualTo equalTo(Expression left, Expression right) {
    return new EqualTo(left, right);
  }

  public static GreaterThan greaterThan(AttributeReference field, int value) {
    return new GreaterThan(field, intLiteral(value));
  }

  public static SortOrder sortOrder(AttributeReference field) {
    return new SortOrder(
        field,
        mock(SortDirection.class),
        mock(NullOrdering.class),
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  @SafeVarargs
  public static <T> Seq<T> asSeq(T... elements) {
    return ScalaConversionUtils.fromList(Arrays.asList(elements));
  }

  public static void mockNewExprId(LongAccumulator id, MockedStatic<NamedExpression> utilities) {
    utilities
        .when(NamedExpression::newExprId)
        .thenAnswer(
            (Answer<ExprId>)
                invocation -> {
                  ExprId exprId = ExprId.apply(id.get());
                  id.accumulate(1);
                  return exprId;
                });
  }

  public static final class AliasBuilder {
    private final Expression expr;

    private AliasBuilder(Expression expr) {
      this.expr = expr;
    }

    public Alias as(String name, ExprId exprId) {
      return new Alias(
          expr,
          name,
          exprId,
          ScalaConversionUtils.asScalaSeqEmpty(),
          Option.empty(),
          ScalaConversionUtils.asScalaSeqEmpty());
    }

    public static AliasBuilder alias(Expression expr) {
      return new AliasBuilder(expr);
    }
  }
}
