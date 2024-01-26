/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.List;
import java.util.Optional;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Builder;
import scala.jdk.CollectionConverters;

public final class ScalaConversions213 implements ScalaConversions {
  @Override
  public <K, V> Map<K, V> toScalaImmutableMap(java.util.Map<K, V> map) {
    Builder<Tuple2<K, V>, Map<K, V>> builder = Map$.MODULE$.<K, V>newBuilder();
    map.forEach((k, v) -> builder.addOne(Tuple2.<K, V>apply(k, v)));
    return builder.result();
  }

  @Override
  public <T> Seq<T> toScalaSeq(List<T> list) {
    Buffer<T> buffer = CollectionConverters.ListHasAsScala(list).asScala();
    return buffer.toSeq();
  }

  @Override
  public <T> scala.collection.immutable.Seq<T> toScalaImmutableSeq(List<T> list) {
    Buffer<T> buffer = CollectionConverters.ListHasAsScala(list).asScala();
    return buffer.toSeq();
  }

  @Override
  public <T> List<T> toJavaList(Seq<T> seq) {
    return CollectionConverters.SeqHasAsJava(seq).asJava();
  }

  @Override
  public <T> List<T> toJavaList(scala.collection.immutable.Seq<T> seq) {
    return CollectionConverters.SeqHasAsJava(seq).asJava();
  }

  @Override
  public <K, V> java.util.Map<K, V> toJavaMap(Map<K, V> map) {
    return CollectionConverters.MapHasAsJava(map).asJava();
  }

  @Override
  public <T> Optional<T> toJavaOptional(Option<T> option) {
    return option.isDefined() ? Optional.ofNullable(option.get()) : Optional.empty();
  }
}
