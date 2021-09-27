package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.runtime.AbstractPartialFunction;

/**
 * Abstract partial function that can potentially extract a list of {@link
 * io.openlineage.client.OpenLineage.Dataset}s (input or output) from a {@link LogicalPlan} node.
 * This is a base class for plan visitors that know how to extract {@link
 * io.openlineage.client.OpenLineage.Dataset}s and their facets from particular {@link LogicalPlan}
 * nodes. A naive implementation of {@link scala.PartialFunction#isDefinedAt(Object)} is
 * implemented, which simply checks that the given {@link LogicalPlan} node is a subclass of the
 * concrete implementation function's generic type parameter.
 *
 * <p>This works for classes that are compiled with a concrete generic type parameter and whose only
 * requirement is that the plan node matches a specified type. E.g., see the {@link
 * InsertIntoDirVisitor} implementation which matches on a {@link LogicalPlan} node that is an
 * instance of {@link org.apache.spark.sql.catalyst.plans.logical.InsertIntoDir}.
 *
 * <p>Nodes that need more complicated inspection of the {@link LogicalPlan} node to determine if
 * the function is defined at that node will need to implement their own {@link
 * scala.PartialFunction#isDefinedAt(Object)} method.
 *
 * @param <T>
 */
public abstract class QueryPlanVisitor<T extends LogicalPlan>
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    Type genericSuperclass = getClass().getGenericSuperclass();
    if (!(genericSuperclass instanceof ParameterizedType)) {
      return false;
    }
    Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
    if (typeArgs != null && typeArgs.length > 0) {
      Type arg = typeArgs[0];
      return ((Class) arg).isAssignableFrom(x.getClass());
    }
    return false;
  }
}
