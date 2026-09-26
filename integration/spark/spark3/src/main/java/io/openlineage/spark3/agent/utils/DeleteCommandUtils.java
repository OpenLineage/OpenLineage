/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;

/**
 * Recognises the Databricks Delta commands that a {@code DELETE FROM} statement is turned into, and
 * reads their target relation.
 *
 * <p>On Databricks the catalyst node {@code DeleteFromTable} never reaches the dataset builders:
 * the runtime resolves the statement straight to a command under {@code
 * com.databricks.sql.transaction.tahoe.commands}, so neither the optimized nor the analyzed plan
 * holds the catalyst node. Those commands are not on the compile classpath, hence the match by
 * class name and the reflective member access, the same approach {@code MergeIntoCommandEdge} and
 * {@code CopyIntoCommand} already use.
 */
@Slf4j
public class DeleteCommandUtils {

  private static final List<String> COMMAND_CLASS_NAMES =
      Arrays.asList(
          "sql.transaction.tahoe.commands.DeleteCommand",
          "sql.transaction.tahoe.commands.DeleteCommandEdge");

  private DeleteCommandUtils() {}

  public static boolean isDeleteCommand(LogicalPlan plan) {
    return matchesCommandClassName(plan.getClass().getCanonicalName());
  }

  /** Package-visible for tests; Databricks command classes are not on the classpath. */
  static boolean matchesCommandClassName(String canonicalName) {
    return canonicalName != null && COMMAND_CLASS_NAMES.stream().anyMatch(canonicalName::endsWith);
  }

  /**
   * Reads the modified relation from {@code target()}, unwrapping the {@link SubqueryAlias} the
   * command keeps when the statement uses a table alias.
   */
  public static Optional<LogicalPlan> target(LogicalPlan plan) {
    Object target;
    try {
      target = MethodUtils.invokeExactMethod(plan, "target", new Object[] {});
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.warn("Cannot extract target from Databricks command {}", plan.getClass(), e);
      return Optional.empty();
    }

    if (target instanceof SubqueryAlias) {
      return Optional.of(((SubqueryAlias) target).child());
    } else if (target instanceof LogicalPlan) {
      return Optional.of((LogicalPlan) target);
    }
    return Optional.empty();
  }
}
