/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import scala.collection.immutable.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.util.Properties;

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

  public static <T> Seq<T> asScalaSeqEmpty() {
    return JavaConverters.asScalaBufferConverter(Collections.<T>emptyList()).asScala().toList();
  }

  public static <K, V> scala.collection.immutable.Map<K, V> asScalaMap(Map<K, V> map) {
    List<Tuple2<K, V>> collect =
        map.entrySet().stream()
            .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    String scalaVersion = Properties.versionNumberString();
    Seq<Tuple2<K, V>> scalaSeq = ScalaConversionUtils.asScalaSeq(collect);
    try {
      if (scalaVersion.startsWith("2.13")) {
        Class<?> aClass = Class.forName("scala.collection.immutable.Map");
        Method asJava = aClass.getMethod("from", Class.forName("scala.collection.IterableOnce"));
        return (scala.collection.immutable.Map<K, V>) asJava.invoke(null, scalaSeq);
      } else {
        Class<?> aClass = Class.forName("scala.collection.immutable.Map$");
        Object module$ = aClass.getField("MODULE$").get(null);
        Method asJava = module$.getClass().getMethod("apply", scala.collection.Seq.class);
        return (scala.collection.immutable.Map<K, V>) asJava.invoke(module$, scalaSeq);
      }
    } catch (NoSuchMethodException
        | ClassNotFoundException
        | InvocationTargetException
        | IllegalAccessException
        | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
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
   * Convert a {@link Seq} to a Java {@link List}.
   *
   * @param seq
   * @param <T>
   * @return
   */
  public static <T> List<T> asJavaCollection(scala.collection.Seq<T> seq) {
    String scalaVersion = Properties.versionNumberString();
    if (scalaVersion.startsWith("2.13")) {
      try {
        Class<?> aClass = Class.forName("scala.jdk.javaapi.CollectionConverters");
        Method asJava = aClass.getMethod("asJava", scala.collection.Seq.class);
        List<T> invoke = (List<T>) asJava.invoke(null, seq);
        return invoke;

      } catch (NoSuchMethodException
          | ClassNotFoundException
          | InvocationTargetException
          | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return JavaConverters.bufferAsJavaListConverter(seq.<T>toBuffer()).asJava();
  }

  public static <T> List<T> asJavaCollection(scala.collection.Set<T> set) {
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
