/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * A utility class for tracking visited nodes in a Spark LogicalPlan, specifically for identifying
 * nodes from which InputDataset information has been extracted. Uses semantic hash codes to
 * uniquely identify nodes representing the same data source within the LogicalPlan's leaf nodes.
 */
public class VisitedNodes {

  /**
   * A map storing event names as keys and sets of semantic hash codes of visited nodes as values.
   */
  private final Map<String, Set<Integer>> visitedNodeHashes = new HashMap<>();

  /**
   * Adds a node's semantic hash code to the set of visited nodes for a given Spark event.
   *
   * @param event the SparkListenerEvent associated with the node
   * @param hashCode the semantic hash code of the node
   */
  public void addVisitedNodeHash(SparkListenerEvent event, int hashCode) {
    String eventName = event.getClass().getSimpleName();
    if (visitedNodeHashes.containsKey(eventName)) {
      visitedNodeHashes.get(eventName).add(hashCode);
    } else {
      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(hashCode);
      visitedNodeHashes.put(eventName, hashSet);
    }
  }

  public void addVisitedNodeHash(SparkListenerEvent event, LogicalPlan p) {
    addVisitedNodeHash(event, p.semanticHash());
  }

  /**
   * Checks if a node with the given semantic hash code has already been visited for a specific
   * Spark event.
   *
   * @param event the SparkListenerEvent associated with the node
   * @param hashCode the semantic hash code of the node
   * @return true if the node has been visited for the event, false otherwise
   */
  public boolean alreadyVisited(SparkListenerEvent event, int hashCode) {
    String eventName = event.getClass().getSimpleName();
    return visitedNodeHashes.containsKey(eventName)
        && visitedNodeHashes.get(eventName).contains(hashCode);
  }

  /** Clears all stored information about visited nodes. */
  public void clearVisitedNodes() {
    visitedNodeHashes.clear();
  }

  /**
   * Checks if a LogicalPlan node has already been visited for a specific Spark event by using its
   * semantic hash.
   *
   * @param event the SparkListenerEvent associated with the node
   * @param logicalPlan the LogicalPlan node to check
   * @return true if the node has been visited for the event, false otherwise
   */
  public boolean alreadyVisited(SparkListenerEvent event, LogicalPlan logicalPlan) {
    return logicalPlan != null && alreadyVisited(event, logicalPlan.semanticHash());
  }
}
