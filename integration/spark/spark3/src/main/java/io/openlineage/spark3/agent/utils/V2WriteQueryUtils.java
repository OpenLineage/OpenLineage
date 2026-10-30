/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.RebalancePartitions;
import org.apache.spark.sql.catalyst.plans.logical.RepartitionOperation;
import org.apache.spark.sql.catalyst.plans.logical.Sort;

/** Helpers for the query of a DataSource V2 write such as {@code ReplaceData}. Spark 3.2+. */
public final class V2WriteQueryUtils {

  private V2WriteQueryUtils() {}

  /**
   * Skips the nodes that Spark's (or Iceberg's) {@code V2Writes} rule puts above a write's query
   * when the table requests a write distribution or ordering, as partitioned Iceberg tables do.
   */
  public static LogicalPlan unwrapWriteQuery(LogicalPlan query) {
    LogicalPlan plan = query;
    while (plan instanceof RepartitionOperation
        || plan instanceof RebalancePartitions
        || plan instanceof Sort) {
      plan = plan.children().head();
    }
    return plan;
  }
}
