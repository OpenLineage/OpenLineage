/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.api;

import scala.runtime.AbstractPartialFunction;

import java.util.Collection;

/**
 * Base implementation of {@link AbstractPartialFunction} which uses {@link
 * AbstractPartial#isDefinedAt(Object)} as the default implementation of the {@link
 * scala.PartialFunction#isDefinedAt(Object)} method.
 *
 * @param <T>
 * @param <F>
 */
abstract class AbstractGenericArgPartialFunction<T, F>
    extends AbstractPartialFunction<T, Collection<F>> implements AbstractPartial<T> {
  @Override
  public boolean isDefinedAt(T x) {
    return AbstractPartial.super.isDefinedAt(x);
  }
}
