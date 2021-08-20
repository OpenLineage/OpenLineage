package openlineage.spark.agent.lifecycle.plan;

import java.util.List;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Singular;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

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
