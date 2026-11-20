/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PythonUDF;
import org.apache.spark.sql.catalyst.plans.logical.BaseEvalPython;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts expression dependencies from BatchEvalPython and ArrowEvalPython operators in {@link
 * LogicalPlan}. Spark's optimizer moves Python and pandas UDF calls out of projections into these
 * operators, which expose each UDF result as a new attribute. Without this visitor the new
 * attribute has no recorded inputs, so a column computed by a Python UDF loses its lineage. Example
 * query 'SELECT my_python_udf(a) AS b FROM t'.
 *
 * <p>The body of a Python UDF is opaque to Spark, so the UDF is treated as a black box, the same
 * way as a ScalaUDF: its result depends on every argument, as a transformation. The UDF itself is
 * never considered masking; an argument keeps the masking of its own expression, such as sha2(a).
 * This is an approximation in some cases:
 *
 * <ul>
 *   <li>Every field of a struct returned by a UDF depends on all arguments of the UDF, also when
 *       the UDF computes a field from only some of them.
 *   <li>An argument that the UDF ignores is still reported as an input.
 *   <li>A UDF that hashes or redacts its arguments is not reported as masking.
 * </ul>
 *
 * <p>Python UDTFs are evaluated by other operators (BatchEvalPythonUDTF and ArrowEvalPythonUDTF,
 * since Spark 3.5) and are not handled here.
 */
public class EvalPythonVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof BaseEvalPython;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    BaseEvalPython node = (BaseEvalPython) operator;
    List<PythonUDF> udfs = ScalaConversionUtils.fromSeq(node.udfs());
    List<Attribute> resultAttrs = ScalaConversionUtils.fromSeq(node.resultAttrs());
    // The n-th result attribute holds the result of the n-th UDF.
    for (int i = 0; i < Math.min(udfs.size(), resultAttrs.size()); i++) {
      Expression udf = udfs.get(i);
      Attribute result = resultAttrs.get(i);
      ExpressionTraverser.of(
              udf,
              result.exprId(),
              ((Expression) result).sql(),
              TransformationInfo.transformation(udf.sql()),
              builder)
          .traverse();
    }
  }
}
