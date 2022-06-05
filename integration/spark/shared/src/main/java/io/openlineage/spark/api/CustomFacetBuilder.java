/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.api;

import java.util.function.BiConsumer;

/**
 * Defines the base class for an event consumer that can generate custom facets from those events. A
 * facet builder is like a partial function, which defines an {@link #isDefinedAt(Object)} method
 * that determines whether the function is defined for a given input. If the function is defined,
 * the {@link #build(Object, BiConsumer)} method is invoked with the input object and a {@link
 * BiConsumer} that accepts both the name of the facet and the facet itself. A {@link
 * CustomFacetBuilder} is itself a {@link BiConsumer} whose {@link #accept(Object, BiConsumer)}
 * method implements the logic described.
 *
 * <p>The event types that can be consumed to generate facets have no common supertype, so the
 * generic argument T isn't bounded. The types of arguments that may be found include
 *
 * <ul>
 *   <li>{@link org.apache.spark.scheduler.StageInfo}
 *   <li>{@link org.apache.spark.scheduler.Stage}
 *   <li>{@link org.apache.spark.rdd.RDD}
 *   <li>{@link org.apache.spark.scheduler.ActiveJob}
 *   <li>{@link org.apache.spark.sql.execution.QueryExecution}
 * </ul>
 *
 * @apiNote This interface is evolving and may change in future releases
 * @param <T>
 * @param <F>
 */
public abstract class CustomFacetBuilder<T, F>
    implements AbstractPartial<Object>, BiConsumer<Object, BiConsumer<String, ? super F>> {

  public final void accept(Object event, BiConsumer<String, ? super F> consumer) {
    if (isDefinedAt(event)) {
      build((T) event, consumer);
    }
  }

  protected abstract void build(T event, BiConsumer<String, ? super F> consumer);
}
