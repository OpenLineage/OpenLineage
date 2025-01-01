/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark2.agent.lifecycle.plan;

import java.util.List;
import scala.Tuple2;

final class ScalaUtils {
  public static <K, V> scala.collection.immutable.Map<K, V> fromTuples(List<Tuple2<K, V>> tuples) {
    scala.collection.immutable.Map<K, V> map = scala.collection.immutable.Map$.MODULE$.empty();
    for (Tuple2<K, V> tuple : tuples) {
      map = map.$plus(Tuple2.apply(tuple._1(), tuple._2()));
    }
    return map;
  }
}
