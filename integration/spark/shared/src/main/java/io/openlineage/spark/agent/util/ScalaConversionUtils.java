/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Set;
import scala.collection.immutable.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

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
    return asScalaSeq(asJavaCollection(seq).stream().map(fn).collect(Collectors.toList()));
  }

  /**
   * Convert a {@link List} to a Scala {@link Seq}.
   *
   * @param list
   * @param <T>
   * @return
   */
  public static <T> Seq<T> asScalaSeq(List<T> list) {
    return JavaConverters.asScalaBufferConverter(list).asScala().toList();
  }

  /**
   * Return an empty {@link Seq}
   *
   * @param <T>
   * @return
   */
  public static <T> Seq<T> asScalaSeqEmpty() {
    return JavaConverters.asScalaBufferConverter(Collections.<T>emptyList()).asScala().toList();
  }

  /**
   * Return an empty {@link Map}
   *
   * @param <K>
   * @param <V>
   * @return
   */
  public static <K, V> scala.collection.immutable.Map<K, V> asScalaMapEmpty() {
    return ScalaConversionUtils.asScalaMap(Collections.emptyMap());
  }

  /**
   * Convert a {@link Map} to a Java {@link scala.collection.immutable.Map}.
   *
   * @param map
   * @param <K>
   * @param <V>
   * @return
   */
  public static <K, V> scala.collection.immutable.Map<K, V> asScalaMap(Map<K, V> map) {
    // Extract the key value of the map in a Scala sequence.
    List<Tuple2<K, V>> collect =
        map.entrySet().stream()
            .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    Seq<Tuple2<K, V>> scalaSeq = ScalaConversionUtils.asScalaSeq(collect);
    // Use the `scala.collection.immutable.Map$.MODULE$` to create an immutable Map
    // This method change between Scala 2.12 and 2.13 but the parameter and the return object are
    // the same.
    return (scala.collection.immutable.Map<K, V>)
        scala.collection.immutable.Map$.MODULE$.apply(scalaSeq);
  }

  /**
   * Convert a {@link Seq} to a Java {@link List}.
   *
   * @param seq
   * @param <T>
   * @return
   */
  public static <T> List<T> asJavaCollection(scala.collection.Seq<T> seq) {
    return JavaConverters.bufferAsJavaListConverter(seq.<T>toBuffer()).asJava();
  }

  /**
   * Convert a {@link Set} to a Java {@link List}.
   *
   * @param set
   * @param <T>
   * @return
   */
  public static <T> List<T> asJavaCollection(Set<T> set) {
    return asJavaCollection(set.toBuffer());
  }

  /**
   * Convert a {@link scala.collection.immutable.Map} to a Java {@link Map}.
   *
   * @param map
   * @param <K>
   * @param <V>
   * @return
   */
  public static <K, V> Map<K, V> asJavaMap(scala.collection.immutable.Map<K, V> map) {
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
