/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * A utility class for tracking visited nodes in a Spark LogicalPlan, specifically for identifying
 * nodes from which InputDataset information has been extracted.
 *
 * <p>Nodes are tracked by object identity (reference equality) rather than semantic hash. This
 * avoids calling {@code LogicalPlan.semanticHash()}, which is expensive on first invocation because
 * it canonicalizes the entire subtree and computes a MurmurHash over it. Within a single query
 * execution, {@code QueryExecution.optimizedPlan()} returns the same object references each time,
 * so identity is sufficient for deduplication. A {@link WeakHashMap} is used so that plan
 * references do not prevent garbage collection once the execution context is released.
 */
public class VisitedNodes {

  private final Map<String, Set<LogicalPlan>> nodesByEvent = new HashMap<>();

  /**
   * Marks a LogicalPlan node as visited for the given Spark event.
   *
   * @param event the SparkListenerEvent associated with the node
   * @param plan the LogicalPlan node to mark as visited
   */
  public void addVisitedNode(SparkListenerEvent event, LogicalPlan plan) {
    String eventName = event.getClass().getSimpleName();
    nodesByEvent
        .computeIfAbsent(eventName, k -> Collections.newSetFromMap(new WeakHashMap<>()))
        .add(plan);
  }

  /**
   * Checks if a LogicalPlan node has already been visited for a specific Spark event.
   *
   * @param event the SparkListenerEvent associated with the node
   * @param plan the LogicalPlan node to check
   * @return true if the node has been visited for the event, false otherwise
   */
  public boolean alreadyVisited(SparkListenerEvent event, LogicalPlan plan) {
    if (plan == null) {
      return false;
    }
    String eventName = event.getClass().getSimpleName();
    Set<LogicalPlan> nodes = nodesByEvent.get(eventName);
    return nodes != null && nodes.contains(plan);
  }

  /** Clears all stored information about visited nodes. */
  public void clearVisitedNodes() {
    nodesByEvent.clear();
  }
}
