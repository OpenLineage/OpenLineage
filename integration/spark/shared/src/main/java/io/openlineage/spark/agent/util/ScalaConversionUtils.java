/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.collection.Set;
import scala.collection.immutable.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

/** Simple conversion utilities for dealing with Scala types */
public final class ScalaConversionUtils implements ScalaConversions {
  private static final class InstanceHolder {
    private static final ScalaConversionUtils INSTANCE = new ScalaConversionUtils();
  }

  private final ScalaConversions delegate;

  private ScalaConversionUtils() {
    this.delegate = ScalaConversionsProvider.getScalaConversions();
  }

  public static ScalaConversionUtils getInstance() {
    return InstanceHolder.INSTANCE;
  }

  @Override
  public <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(Map<K, V> map) {
    return delegate.toScalaImmutableMap(map);
  }

  @Override
  public <T> scala.collection.Seq<T> toScalaSeq(List<T> list) {
    return delegate.toScalaSeq(list);
  }

  @Override
  public <T> Seq<T> toScalaImmutableSeq(List<T> list) {
    return delegate.toScalaImmutableSeq(list);
  }

  @Override
  public <T> List<T> toJavaList(scala.collection.Seq<T> seq) {
    return delegate.toJavaList(seq);
  }

  @Override
  public <T> List<T> toJavaList(scala.collection.immutable.Seq<T> seq) {
    return delegate.toJavaList(seq);
  }

  @Override
  public <K, V> Map<K, V> toJavaMap(scala.collection.immutable.Map<K, V> map) {
    return delegate.toJavaMap(map);
  }

  @Override
  public <T> Optional<T> toJavaOptional(Option<T> option) {
    return delegate.toJavaOptional(option);
  }

  public static <T> scala.collection.immutable.Seq<T> fromList(List<T> list) {
    return getInstance().toScalaImmutableSeq(list);
  }

  public static <T> scala.collection.immutable.Seq<T> asScalaSeqEmpty() {
    return getInstance().emptyScalaImmutableSeq();
  }

  public static <K, V> scala.collection.immutable.Map<K, V> asScalaMapEmpty() {
    return getInstance().emptyScalaImmutableMap();
  }

  public static <K, V> scala.collection.immutable.Map<K, V> fromJavaMap(Map<K, V> map) {
    return getInstance().toScalaImmutableMap(map);
  }

  public static <T> List<T> fromSeq(scala.collection.Seq<T> seq) {
    return getInstance().toJavaList(seq);
  }

  public static <T> List<T> fromSet(Set<T> set) {
    return fromSeq(set.toBuffer());
  }

  public static <K, V> Map<K, V> fromMap(scala.collection.immutable.Map<K, V> map) {
    return getInstance().toJavaMap(map);
  }

  public static <T> Optional<T> asJavaOptional(Option<T> opt) {
    return getInstance().toJavaOptional(opt);
  }

  public static <T> Function0<T> toScalaFn(Supplier<T> supplier) {
    return new AbstractFunction0<T>() {
      @Override
      public T apply() {
        return supplier.get();
      }
    };
  }

  public static <T, R> Function1<T, R> toScalaFn(Function<T, R> fn) {
    return new AbstractFunction1<T, R>() {
      @Override
      public R apply(T arg) {
        return fn.apply(arg);
      }
    };
  }
}
