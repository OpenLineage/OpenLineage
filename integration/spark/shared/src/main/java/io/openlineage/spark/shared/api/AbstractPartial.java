/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.api;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * Non-public interface - a generalization of {@link scala.PartialFunction$} that can be extended
 * for {@link java.util.function.Consumer}s as well as {@link java.util.function.Function}s.
 *
 * <p>A naive default implementation of {@link #isDefinedAt(Object)} is defined, which relies on
 * compiled generic type parameters in the concrete subclasses to determine whether an object can be
 * handled. If the concrete subclass is compiled with a concrete type argument, then {@link
 * #isDefinedAt(Object)} will return true if its input parameter is assignable to the type argument.
 *
 * @param <T>
 */
interface AbstractPartial<T> {

  default boolean isDefinedAt(T x) {
    Type genericSuperclass = getClass().getGenericSuperclass();
    if (!(genericSuperclass instanceof ParameterizedType)) {
      return false;
    }
    Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
    if (typeArgs != null && typeArgs.length > 0) {
      Type arg = typeArgs[0];
      if (arg instanceof TypeVariable) {
        return false;
      }
      return ((Class) arg).isAssignableFrom(x.getClass());
    }
    return false;
  }
}
