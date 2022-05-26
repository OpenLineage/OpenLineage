package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.spark.agent.lifecycle.UnknownEntryFacetListener;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

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

  private final boolean searchDependencies;

  public AbstractQueryPlanDatasetBuilder(OpenLineageContext context, boolean searchDependencies) {
    this.context = context;
    this.searchDependencies = searchDependencies;
  }

  protected DatasetFactory<OpenLineage.OutputDataset> outputDataset() {
    return DatasetFactory.output(context);
  }

  protected DatasetFactory<OpenLineage.InputDataset> inputDataset() {
    return DatasetFactory.input(context);
  }

  public abstract List<D> apply(P p);

  public final List<D> apply(T event) {
    return context
        .getQueryExecution()
        .map(
            qe -> {
              QueryPlanVisitor<LogicalPlan, D> visitor = asQueryPlanVisitor(event);
              if (searchDependencies) {
                return ScalaConversionUtils.fromSeq(qe.optimizedPlan().collect(visitor)).stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
              } else if (visitor.isDefinedAt(qe.optimizedPlan())) {
                return visitor.apply(qe.optimizedPlan());
              } else {
                return Collections.<D>emptyList();
              }
            })
        .orElseGet(Collections::emptyList);
  }

  public <L extends LogicalPlan> QueryPlanVisitor<L, D> asQueryPlanVisitor(T event) {
    AbstractQueryPlanDatasetBuilder<T, P, D> builder = this;
    return new QueryPlanVisitor<L, D>(context) {
      @Override
      public boolean isDefinedAt(LogicalPlan x) {
        return builder.isDefinedAt(event) && isDefinedAtLogicalPlan(x);
      }

      @Override
      public List<D> apply(LogicalPlan x) {
        unknownEntryFacetListener.accept(x);
        return builder.apply(event, (P) x);
      }
    };
  }

  protected List<D> apply(T event, P plan) {
    return apply(plan);
  }

  protected PartialFunction<LogicalPlan, Collection<D>> delegate(
      Collection<PartialFunction<LogicalPlan, List<D>>> visitors,
      Collection<? extends PartialFunction<Object, Collection<D>>> builders,
      T event) {

    return PlanUtils.merge(
        Stream.concat(
                visitors.stream(),
                builders.stream()
                    .filter(
                        builder ->
                            builder instanceof AbstractQueryPlanDatasetBuilder
                                && builder.isDefinedAt(event))
                    .map(
                        builder ->
                            ((AbstractQueryPlanDatasetBuilder<Object, LogicalPlan, D>) builder)
                                .asQueryPlanVisitor(event)))
            .collect(Collectors.toList()));
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
      if (arg instanceof TypeVariable) {
        return false;
      }
      return ((Class) arg).isAssignableFrom(logicalPlan.getClass());
    }
    return false;
  }
}
