package io.openlineage.spark.extension.v1;

import java.util.Objects;

/**
 * Class to contain reference to expression nodes in a Spark plan. Input expression (or expressions)
 * have to be extracted from expression object
 */
public class ExpressionDependencyWithDelegate implements ExpressionDependency {
  private final OlExprId outputExprId;
  private final Object expression;

  public ExpressionDependencyWithDelegate(OlExprId outputExprId, Object expression) {
    this.outputExprId = outputExprId;
    this.expression = expression;
  }

  @Override
  public OlExprId getOutputExprId() {
    return outputExprId;
  }

  public Object getExpression() {
    return expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExpressionDependencyWithDelegate that = (ExpressionDependencyWithDelegate) o;
    return Objects.equals(outputExprId, that.outputExprId)
        && Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(outputExprId, expression);
  }
}
