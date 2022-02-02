/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDirVisitor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractPartialFunction;

/**
 * Abstract partial function that can potentially extract a list of {@link
 * io.openlineage.client.OpenLineage.Dataset}s (input or output) from a {@link LogicalPlan} node.
 * This is a base class for plan visitors that know how to extract {@link
 * io.openlineage.client.OpenLineage.Dataset}s and their facets from particular {@link LogicalPlan}
 * nodes.
 *
 * <p>A non-null {@link OpenLineageContext} instance is a required constructor argument. The {@link
 * OpenLineageContext} offers access to a configured {@link OpenLineage} instance, for constructing
 * OpenLineage model instances with set producer URIs, the current {@link
 * org.apache.spark.sql.SparkSession} (if set), the {@link org.apache.spark.SparkContext}, and other
 * existing {@link QueryPlanVisitor}s for delegating visitors.
 *
 * <p>Subclasses should use the {@link OpenLineage} client returned by calling {@code
 * getOpenLineage()} defined in the class {@link OpenLineageContext} to construct all model objects
 * to that the correct producer and schema URIs are set. For convenience, {@link #outputDataset()}
 * and {@link #inputDataset()} methods have been implemented for subclasses that need to construct
 * one or the other.
 *
 * <p>A naive implementation of {@link scala.PartialFunction#isDefinedAt(Object)} is implemented,
 * which simply checks that the given {@link LogicalPlan} node is a subclass of the concrete
 * implementation function's generic type parameter.
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
public abstract class QueryPlanVisitor<T extends LogicalPlan, D extends OpenLineage.Dataset>
    extends AbstractPartialFunction<LogicalPlan, List<D>> {
  @NonNull protected final OpenLineageContext context;

  protected SparkListenerEvent triggeringEvent;

  protected QueryPlanVisitor(@NonNull OpenLineageContext context) {
    this.context = context;
  }

  protected DatasetFactory<OpenLineage.OutputDataset> outputDataset() {
    return DatasetFactory.output(context.getOpenLineage());
  }

  protected DatasetFactory<OpenLineage.InputDataset> inputDataset() {
    return DatasetFactory.input(context.getOpenLineage());
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());

  protected Optional<CatalogTable> catalogTableFor(TableIdentifier tableId) {
    return context
        .getSparkSession()
        .flatMap(
            session -> {
              try {
                return Optional.of(session.sessionState().catalog().getTableMetadata(tableId));
              } catch (Exception e) {
                logger.warn("Unable to find table by identifier {} - {}", tableId, e.getMessage());
                return Optional.empty();
              }
            });
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    Type genericSuperclass = getClass().getGenericSuperclass();
    if (!(genericSuperclass instanceof ParameterizedType)) {
      return false;
    }
    Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
    if (typeArgs != null && typeArgs.length > 0) {
      Type arg = typeArgs[0];
      boolean isAssignable = ((Class) arg).isAssignableFrom(x.getClass());
      if (isAssignable) {
        logger.info("Matched {} to logical plan {}", this, x);
      }
      return isAssignable;
    }
    return false;
  }

  public void setTriggeringEvent(SparkListenerEvent triggeringEvent) {
    this.triggeringEvent = triggeringEvent;
  }

  @Override
  public String toString() {
    Type genericSuperclass = getClass().getGenericSuperclass();
    Stream<Optional<String>> typeArgs;
    if (!(genericSuperclass instanceof ParameterizedType)) {
      typeArgs = Stream.of(Optional.empty(), Optional.empty());
    } else {
      Type[] actualArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
      typeArgs =
          Arrays.stream(actualArgs).map(arg -> Optional.ofNullable(arg).map(Type::getTypeName));
    }
    String genericArgs = typeArgs.map(opt -> opt.orElse("?")).collect(Collectors.joining(","));
    return getClass().getName() + "<" + genericArgs + ">";
  }
}
