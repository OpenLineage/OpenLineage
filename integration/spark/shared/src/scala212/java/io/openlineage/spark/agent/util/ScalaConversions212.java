/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.List;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Builder;

public final class ScalaConversions212 implements ScalaConversions {
  @Override
  public <K, V> Map<K, V> toScalaImmutableMap(java.util.@NotNull Map<K, V> map) {
    Builder<Tuple2<K, V>, Map<K, V>> builder = Map$.MODULE$.<K, V>newBuilder();
    map.forEach((k, v) -> builder.$plus$eq(Tuple2.<K, V>apply(k, v)));
    return builder.result();
  }

  @Override
  public <T> scala.collection.Seq<T> toScalaSeq(@NotNull List<T> list) {
    Builder<T, scala.collection.Seq<T>> builder = scala.collection.Seq$.MODULE$.<T>newBuilder();
    list.forEach(builder::$plus$eq);
    return builder.result();
  }

  @Override
  public <T> scala.collection.immutable.Seq<T> toScalaImmutableSeq(@NotNull List<T> list) {
    Builder<T, scala.collection.immutable.Seq<T>> builder =
        scala.collection.immutable.Seq$.MODULE$.<T>newBuilder();
    list.forEach(builder::$plus$eq);
    return builder.result();
  }

  @Override
  public <T> List<T> toJavaList(scala.collection.Seq<T> seq) {
    return JavaConverters.<T>seqAsJavaList(seq);
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public <T> List<T> toJavaList(scala.collection.immutable.Seq<T> seq) {
    return JavaConverters.<T>seqAsJavaList(seq);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> java.util.Map<K, V> toJavaMap(Map<K, V> map) {
    return JavaConverters.<K, V>mapAsJavaMap(map);
  }

  @Override
  public <T> Optional<T> toJavaOptional(@NotNull Option<T> option) {
    return option.isDefined() ? Optional.ofNullable(option.get()) : Optional.empty();
  }
}
