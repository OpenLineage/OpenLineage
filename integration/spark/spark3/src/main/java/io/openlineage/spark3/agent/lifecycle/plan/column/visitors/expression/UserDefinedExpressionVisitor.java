/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static java.util.Objects.nonNull;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UserDefinedExpression;

/**
 * Visitor that extracts lineage from user defined functions, i.e. any Spark {@link
 * UserDefinedExpression}.
 *
 * <p>This covers - among others - {@code ScalaUDF} (including the Java UDF API), {@code PythonUDF}
 * and {@code PythonUDAF} (through {@code PythonFuncExpression}), {@code ScalaUDAF}, {@code
 * ApplyFunctionExpression} (DSv2 {@code ScalarFunction}) and the Hive UDF/UDAF/UDTF wrappers.
 *
 * <p>Without this visitor a UDF is handled by the generic fallback of {@link ExpressionTraverser},
 * which emits a {@link TransformationInfo.Types#DIRECT}, {@link
 * TransformationInfo.Subtypes#TRANSFORMATION} edge for every child. That claims more than we know:
 * the body of a UDF is opaque to Spark, so such an edge is indistinguishable from a well understood
 * expression like {@code upper(name)} and gives consumers false confidence that the output value can
 * be traced back to the input value.
 *
 * <p>Instead every child is recorded as an {@link TransformationInfo.Types#INDIRECT}, {@link
 * TransformationInfo.Subtypes#TRANSFORMATION} dependency: the input demonstrably influenced the
 * output, but the nature of the influence is unknown. The name of the function - when Spark exposes
 * one - is carried in the transformation description so consumers can tell which function severed
 * the direct traceability.
 *
 * <p>Masking is reported as {@code false} because it cannot be determined: a UDF may or may not
 * obfuscate its input, and asserting {@code true} would be as wrong as the DIRECT edge this visitor
 * replaces.
 */
@Slf4j
public class UserDefinedExpressionVisitor implements ExpressionVisitor {

  /** Description used when Spark does not expose a usable function name. */
  static final String DESCRIPTION = "UDF";

  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof UserDefinedExpression;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    if (!nonNull(expression.children())) {
      return;
    }
    TransformationInfo transformationInfo = transformationInfoOf(expression);
    ScalaConversionUtils.fromSeq(expression.children())
        .forEach(child -> traverser.copyFor(child, transformationInfo).traverse());
  }

  private static TransformationInfo transformationInfoOf(Expression expression) {
    return new TransformationInfo(
        TransformationInfo.Types.INDIRECT,
        TransformationInfo.Subtypes.TRANSFORMATION,
        describe(expression),
        false);
  }

  /**
   * Builds the transformation description, e.g. {@code UDF: my_function}. Falls back to plain {@link
   * #DESCRIPTION} when the name is absent or when it is just Spark's own generic placeholder.
   */
  private static String describe(Expression expression) {
    try {
      String name = ((UserDefinedExpression) expression).name();
      if (name == null) {
        return DESCRIPTION;
      }
      String trimmed = name.trim();
      if (trimmed.isEmpty() || DESCRIPTION.equalsIgnoreCase(trimmed)) {
        return DESCRIPTION;
      }
      return DESCRIPTION + ": " + trimmed;
    } catch (RuntimeException | LinkageError e) {
      log.debug("Could not resolve name of user defined expression {}", expression.getClass(), e);
      return DESCRIPTION;
    }
  }
}
