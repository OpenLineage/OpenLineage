/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Recognises the Databricks Delta commands that a {@code COPY INTO} statement is turned into.
 *
 * <p>On newer Databricks runtimes (Spark 4.0+) the logical plan is {@code CopyIntoCommandEdge}
 * rather than {@code CopyIntoCommand}. Neither class is on the compile classpath, so builders match
 * by canonical class name suffix, the same approach used for other tahoe command nodes.
 */
public class CopyIntoCommandUtils {

  private static final List<String> COMMAND_CLASS_NAMES =
      Arrays.asList(
          "sql.transaction.tahoe.commands.CopyIntoCommand",
          "sql.transaction.tahoe.commands.CopyIntoCommandEdge");

  private CopyIntoCommandUtils() {}

  public static boolean isCopyIntoCommand(LogicalPlan plan) {
    return matchesCommandClassName(plan.getClass().getCanonicalName());
  }

  /** Package-visible for tests; Databricks command classes are not on the classpath. */
  static boolean matchesCommandClassName(String canonicalName) {
    return canonicalName != null && COMMAND_CLASS_NAMES.stream().anyMatch(canonicalName::endsWith);
  }
}
