package openlineage.spark.agent.lifecycle.plan;

import java.util.List;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Singular;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} visitor whose sole purpose is to
 * send visited nodes to a list of node {@link Consumer}s that need to track plan nodes.
 *
 * @param <T>
 * @param <R>
 */
@Builder
public class PlanTraversal<T, R> extends AbstractPartialFunction<T, R> {

  PartialFunction<T, R> processor;
  @Singular List<Consumer<T>> visitedNodeListeners;

  @Override
  public R apply(T element) {
    R res = processor.apply(element);
    visitedNodeListeners.forEach(c -> c.accept(element));
    return res;
  }

  @Override
  public boolean isDefinedAt(T x) {
    return processor.isDefinedAt(x);
  }
}
