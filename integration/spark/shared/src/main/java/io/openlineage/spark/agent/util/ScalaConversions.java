/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.List;
import java.util.Optional;
import scala.Option;

public interface ScalaConversions {
  <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> map);

  <T> scala.collection.Seq<T> toScalaSeq(java.util.List<T> list);

  <T> scala.collection.immutable.Seq<T> toScalaImmutableSeq(java.util.List<T> list);

  <T> List<T> toJavaList(scala.collection.Seq<T> seq);

  <T> List<T> toJavaList(scala.collection.immutable.Seq<T> seq);

  <K, V> java.util.Map<K, V> toJavaMap(scala.collection.immutable.Map<K, V> map);

  @SuppressWarnings("unchecked")
  default <T> scala.collection.immutable.Seq<T> emptyScalaImmutableSeq() {
    return (scala.collection.immutable.Seq<T>) scala.collection.immutable.Seq$.MODULE$.<T>empty();
  }

  default <K, V> scala.collection.immutable.Map<K, V> emptyScalaImmutableMap() {
    return scala.collection.immutable.Map$.MODULE$.<K, V>empty();
  }

  <T> Optional<T> toJavaOptional(Option<T> option);
}
