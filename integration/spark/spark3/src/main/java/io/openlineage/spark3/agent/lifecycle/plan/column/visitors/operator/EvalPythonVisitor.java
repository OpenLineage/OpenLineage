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
import org.apache.spark.sql.catalyst.plans.logical.ArrowEvalPython;
import org.apache.spark.sql.catalyst.plans.logical.BatchEvalPython;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts expression dependencies from BatchEvalPython and ArrowEvalPython operators in {@link
 * LogicalPlan}. Spark's optimizer moves Python and pandas UDF calls out of projections into these
 * operators, which expose each UDF result as a new attribute. Without this visitor the new
 * attribute has no recorded inputs, so a column computed by a Python UDF loses its lineage. Example
 * query 'SELECT my_python_udf(a) AS b FROM t'.
 */
public class EvalPythonVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof BatchEvalPython || operator instanceof ArrowEvalPython;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    List<PythonUDF> udfs;
    List<Attribute> resultAttrs;
    if (operator instanceof BatchEvalPython) {
      BatchEvalPython node = (BatchEvalPython) operator;
      udfs = ScalaConversionUtils.fromSeq(node.udfs());
      resultAttrs = ScalaConversionUtils.fromSeq(node.resultAttrs());
    } else {
      ArrowEvalPython node = (ArrowEvalPython) operator;
      udfs = ScalaConversionUtils.fromSeq(node.udfs());
      resultAttrs = ScalaConversionUtils.fromSeq(node.resultAttrs());
    }
    // The n-th result attribute holds the result of the n-th UDF.
    for (int i = 0; i < Math.min(udfs.size(), resultAttrs.size()); i++) {
      Expression udf = udfs.get(i);
      Attribute result = resultAttrs.get(i);
      ScalaConversionUtils.<Expression>fromSeq(udf.children())
          .forEach(
              argument ->
                  ExpressionTraverser.of(
                          argument,
                          result.exprId(),
                          ((Expression) result).sql(),
                          TransformationInfo.transformation(udf.sql()),
                          builder)
                      .traverse());
    }
  }
}
