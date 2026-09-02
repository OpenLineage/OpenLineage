/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.Option;

/** Tracks whether a Spark SQL execution was started inside another SQL execution. */
@Slf4j
public class SparkSqlExecutionNestingTracker {

  private static final String[] DELTA_CALL_SITE_MARKERS = {
    "org.apache.spark.sql.delta.", "com.databricks.sql.transaction.tahoe.", "io.delta."
  };

  private final Map<Long, ActiveExecution> activeExecutions = new LinkedHashMap<>();

  /**
   * Registers a SQL execution and returns whether it is known to be nested.
   *
   * <p>Spark 3.4 and newer expose the root execution ID directly. Older Spark releases do not, so
   * the fallback deliberately recognizes only a narrow set of unambiguous shapes: a non-command
   * execution started while exactly one command is active, with either the same user call site or a
   * Delta implementation frame. Ambiguous cases fail open and keep lineage events.
   */
  public synchronized boolean register(
      SparkListenerSQLExecutionStart event, QueryExecution queryExecution) {
    return register(event, queryExecution, rootExecutionId(event));
  }

  public synchronized boolean register(
      SparkListenerSQLExecutionStart event,
      QueryExecution queryExecution,
      OptionalLong rootExecutionId) {
    boolean command = isCommand(queryExecution);
    boolean legacyNested = !rootExecutionId.isPresent() && isLegacyNestedExecution(event, command);
    boolean nested =
        rootExecutionId.isPresent()
            ? rootExecutionId.getAsLong() != event.executionId()
            : legacyNested;
    boolean commandChild =
        rootExecutionId.isPresent()
            ? isNestedUnderCommand(event.executionId(), rootExecutionId.getAsLong())
            : legacyNested;

    activeExecutions.put(
        event.executionId(),
        new ActiveExecution(
            command, safe(event.description()), safe(event.details()), nested, commandChild));
    return nested;
  }

  public synchronized boolean isNested(long executionId) {
    ActiveExecution execution = activeExecutions.get(executionId);
    return execution != null && execution.nested;
  }

  /** Returns whether the execution is a child of a root command. */
  public synchronized boolean isCommandChild(long executionId) {
    ActiveExecution execution = activeExecutions.get(executionId);
    return execution != null && execution.commandChild;
  }

  public synchronized void end(long executionId) {
    activeExecutions.remove(executionId);
  }

  public synchronized void clear() {
    activeExecutions.clear();
  }

  /** Reads Spark 3.4+'s root execution ID without linking older Spark runtimes to that method. */
  public static OptionalLong rootExecutionId(SparkListenerSQLExecutionStart event) {
    try {
      Object value = event.getClass().getMethod("rootExecutionId").invoke(event);
      if (value instanceof Option && ((Option<?>) value).isDefined()) {
        return OptionalLong.of(((Number) ((Option<?>) value).get()).longValue());
      }
    } catch (NoSuchMethodException e) {
      // Spark 3.1-3.3 do not expose rootExecutionId.
    } catch (Exception | LinkageError e) {
      log.debug("Unable to read Spark SQL root execution ID", e);
    }
    return OptionalLong.empty();
  }

  private boolean isNestedUnderCommand(long executionId, long rootExecutionId) {
    if (executionId == rootExecutionId) {
      return false;
    }
    ActiveExecution root = activeExecutions.get(rootExecutionId);
    return root != null && root.command;
  }

  private boolean isLegacyNestedExecution(SparkListenerSQLExecutionStart event, boolean command) {
    if (command) {
      // A command can be a concurrent top-level write. Without a root ID, suppressing it is unsafe.
      return false;
    }

    ActiveExecution commandRoot = null;
    for (ActiveExecution execution : activeExecutions.values()) {
      if (!execution.command) {
        continue;
      }
      if (commandRoot != null) {
        // More than one active command makes the possible parent ambiguous.
        return false;
      }
      commandRoot = execution;
    }
    if (commandRoot == null) {
      return false;
    }

    String description = safe(event.description());
    String details = safe(event.details());
    if (!details.isEmpty()
        && details.equals(commandRoot.details)
        && description.equals(commandRoot.description)) {
      return true;
    }

    for (String marker : DELTA_CALL_SITE_MARKERS) {
      if (details.contains(marker)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isCommand(QueryExecution queryExecution) {
    try {
      return queryExecution != null && queryExecution.optimizedPlan() instanceof Command;
    } catch (Exception | LinkageError e) {
      log.debug("Unable to determine whether a Spark SQL execution is a command", e);
      return false;
    }
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  private static class ActiveExecution {
    private final boolean command;
    private final String description;
    private final String details;
    private final boolean nested;
    private final boolean commandChild;

    private ActiveExecution(
        boolean command, String description, String details, boolean nested, boolean commandChild) {
      this.command = command;
      this.description = description;
      this.details = details;
      this.nested = nested;
      this.commandChild = commandChild;
    }
  }
}
