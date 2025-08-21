/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import scala.Option;
import scala.collection.immutable.Seq;

public class ColumnLevelFixtures {
  public static final String NAME_1 = "name1";
  public static final String NAME_2 = "name2";

  public static final ExprId EXPR_ID_1 = ExprId.apply(21);
  public static final ExprId EXPR_ID_2 = ExprId.apply(22);

  public static AttributeReference field(String name, ExprId exprId) {
    return new AttributeReference(
        name,
        IntegerType$.MODULE$,
        false,
        Metadata$.MODULE$.empty(),
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  @SafeVarargs
  public static <T> Seq<T> asSeq(T... elements) {
    return ScalaConversionUtils.fromList(Arrays.asList(elements));
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
