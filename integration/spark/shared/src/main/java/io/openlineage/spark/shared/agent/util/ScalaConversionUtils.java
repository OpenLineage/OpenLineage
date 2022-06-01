/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.util;

import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/** Simple conversion utilities for dealing with Scala types */
public class ScalaConversionUtils {

  /**
   * Apply a map function to a {@link Seq}. This consolidates the silliness in converting between
   * Scala and Java collections.
   *
   * @param seq
   * @param fn
   * @param <R>
   * @param <T>
   * @return
   */
  public static <R, T> Seq<R> map(Seq<T> seq, Function<T, R> fn) {
    return fromList(fromSeq(seq).stream().map(fn).collect(Collectors.toList()));
  }

  /**
   * Convert a {@link List} to a Scala {@link Seq}.
   *
   * @param list
   * @param <T>
   * @return
   */
  public static <T> Seq<T> fromList(List<T> list) {
    return JavaConverters.asScalaBufferConverter(list).asScala();
  }

  /**
   * Convert a {@link Seq} to a Java {@link List}.
   *
   * @param seq
   * @param <T>
   * @return
   */
  public static <T> List<T> fromSeq(Seq<T> seq) {
    return JavaConverters.bufferAsJavaListConverter(seq.<T>toBuffer()).asJava();
  }

  /**
   * Convert a {@link scala.collection.immutable.Map} to a Java {@link Map}.
   *
   * @param map
   * @param <K>
   * @param <V>
   * @return
   */
  public static <K, V> Map<K, V> fromMap(scala.collection.immutable.Map<K, V> map) {
    return JavaConverters.mapAsJavaMapConverter(map).asJava();
  }

  /**
   * Convert a Scala {@link Option} to a Java {@link Optional}.
   *
   * @param opt
   * @param <T>
   * @return
   */
  public static <T> Optional<T> asJavaOptional(Option<T> opt) {
    return Optional.ofNullable(
        opt.getOrElse(
            new AbstractFunction0<T>() {
              @Override
              public T apply() {
                return null;
              }
            }));
  }

  public static <T> Collector<T, ?, Seq<T>> toSeq() {
    return Collector.of(
        Seq$.MODULE$::newBuilder,
        Builder::$plus$eq,
        (Builder<T, Seq<T>> a, Builder<T, Seq<T>> b) ->
            (Builder<T, Seq<T>>) a.$plus$plus$eq(b.result()),
        Builder::result);
  }

  /**
   * Convert a {@link Supplier} to a Scala {@link Function0}
   *
   * @param supplier
   * @param <T>
   * @return
   */
  public static <T> Function0<T> toScalaFn(Supplier<T> supplier) {
    return new AbstractFunction0<T>() {
      @Override
      public T apply() {
        return supplier.get();
      }
    };
  }

  /**
   * Convert a {@link Function} to a Scala {@link Function1}
   *
   * @param fn
   * @param <T>
   * @param <R>
   * @return
   */
  public static <T, R> Function1<T, R> toScalaFn(Function<T, R> fn) {
    return new AbstractFunction1<T, R>() {
      @Override
      public R apply(T arg) {
        return fn.apply(arg);
      }
    };
  }
}
