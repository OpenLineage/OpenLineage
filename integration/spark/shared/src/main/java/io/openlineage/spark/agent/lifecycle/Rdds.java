/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.lifecycle.proxy.RddProxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.Dependency;
import org.apache.spark.rdd.RDD;

public final class Rdds {
  private Rdds() {}

  public static Set<RDD<?>> flattenRDDs(RDD<?> rdd) {
    RddProxy rddProxy = new RddProxy(rdd);
    Set<RddProxy> rddProxies = doFlatten(rddProxy);
    return rddProxies.stream().map(RddProxy::proxiedRDD).collect(Collectors.toSet());
  }

  private static Set<RddProxy> doFlatten(RddProxy rdd) {
    Set<RddProxy> rdds = new HashSet<>();
    rdds.add(rdd);
    if (rdd.isShuffledRowRdd()) {
      rdds.addAll(doFlatten(new RddProxy(rdd.dependency().rdd())));
    }

    List<Dependency<?>> deps = rdd.dependencies();
    for (Dependency<?> dep : deps) {
      rdds.addAll(doFlatten(new RddProxy(dep.rdd())));
    }
    return rdds;
  }

  public static List<RDD<?>> findFileLikeRdds(RDD<?> rdd) {
    RddProxy proxy = new RddProxy(rdd);
    List<RddProxy> rddProxies = doFindFileLikeRdds(proxy);
    return rddProxies.stream().map(RddProxy::proxiedRDD).collect(Collectors.toList());
  }

  private static List<RddProxy> doFindFileLikeRdds(RddProxy rdd) {
    List<RddProxy> ret = new ArrayList<>();
    Deque<RddProxy> deps = new ArrayDeque<>();
    deps.add(rdd);
    while (!deps.isEmpty()) {
      RddProxy cur = deps.pop();
      deps.addAll(
          cur.dependencies().stream()
              .map(Dependency::rdd)
              .map(RddProxy::new)
              .collect(Collectors.toList()));
      if (cur.isHadoopRdd() || cur.isFileScanRdd()) {
        ret.add(cur);
      }
    }
    return ret;
  }
}
