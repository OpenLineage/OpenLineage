package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.spark.agent.lifecycle.UnknownEntryFacetListener;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link AbstractQueryPlanDatasetBuilder} serves as a bridge between the Abstract*DatasetBuilders
 * and {@link QueryPlanVisitor}s. This base class provides the capability to trigger on a specific
 * {@link org.apache.spark.scheduler.SparkListenerEvent} rather than visiting the query plan for
 * every spark event (such as {@link org.apache.spark.scheduler.SparkListenerStageSubmitted}).
 * Additionally, the triggering {@link org.apache.spark.scheduler.SparkListenerEvent} can be
 * utilized during processing of the {@link LogicalPlan} nodes (e.g., reading Spark properties set
 * in {@link SparkListenerJobStart#properties()}).
 *
 * @param <T>
 * @param <P>
 * @param <D>
 */
public abstract class AbstractQueryPlanDatasetBuilder<T, P extends LogicalPlan, D extends Dataset>
    extends AbstractGenericArgPartialFunction<T, D> {
  protected final OpenLineageContext context;
  private final UnknownEntryFacetListener unknownEntryFacetListener =
      UnknownEntryFacetListener.getInstance();

  public AbstractQueryPlanDatasetBuilder(OpenLineageContext context) {
    this.context = context;
  }

  protected DatasetFactory<OpenLineage.OutputDataset> outputDataset() {
    return DatasetFactory.output(context.getOpenLineage());
  }

  protected DatasetFactory<OpenLineage.InputDataset> inputDataset() {
    return DatasetFactory.input(context.getOpenLineage());
  }

  public abstract List<D> apply(P p);

  public List<D> apply(T event) {
    return context
        .getQueryExecution()
        .map(
            qe ->
                ScalaConversionUtils.fromSeq(qe.optimizedPlan().collect(asQueryPlanVisitor(event)))
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()))
        .orElseGet(Collections::emptyList);
  }

  public <L extends LogicalPlan> QueryPlanVisitor<L, D> asQueryPlanVisitor(T event) {
    AbstractQueryPlanDatasetBuilder<T, P, D> builder = this;
    return new QueryPlanVisitor<L, D>(context) {
      @Override
      public boolean isDefinedAt(LogicalPlan x) {
        return isDefinedAtLogicalPlan(x);
      }

      @Override
      public List<D> apply(LogicalPlan x) {
        unknownEntryFacetListener.accept(x);
        return builder.apply((P) x);
      }
    };
  }

  /**
   * Similar to the logic in {@link AbstractPartial}, this reads the type from the <i>second</i>
   * generic argument on the class, if it is present and non-null.
   *
   * @param logicalPlan
   * @return
   */
  protected boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    Type genericSuperclass = getClass().getGenericSuperclass();
    if (!(genericSuperclass instanceof ParameterizedType)) {
      return false;
    }
    Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
    if (typeArgs != null && typeArgs.length > 1) {
      Type arg = typeArgs[1];
      return ((Class) arg).isAssignableFrom(logicalPlan.getClass());
    }
    return false;
  }
}
