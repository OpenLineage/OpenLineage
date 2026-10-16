/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Reflection based helpers for Hudi relations. Hudi classes are not guaranteed to be present on the
 * classpath, so they are accessed by name only.
 */
@Slf4j
public class HudiUtils {
  private static final String HOODIE_BASE_RELATION_CLASS = "org.apache.hudi.HoodieBaseRelation";
  private static final String INSERT_INTO_HOODIE_TABLE_COMMAND_CLASS =
      "org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand";

  private HudiUtils() {}

  /** Checks whether the relation is a Hudi {@code HoodieBaseRelation} (e.g. MOR or incremental). */
  public static boolean isHudiBaseRelation(Object relation) {
    return isInstanceOf(relation, HOODIE_BASE_RELATION_CLASS);
  }

  /**
   * Checks whether the plan is Hudi's {@code InsertIntoHoodieTableCommand}, which Hudi SQL uses for
   * {@code INSERT INTO/OVERWRITE} on Hudi tables. It is a leaf command, so its target and query are
   * not reachable through {@link LogicalPlan#children()}.
   */
  public static boolean isInsertIntoHoodieTableCommand(LogicalPlan plan) {
    return isInstanceOf(plan, INSERT_INTO_HOODIE_TABLE_COMMAND_CLASS);
  }

  /** Returns the target relation of an {@code InsertIntoHoodieTableCommand}. */
  public static Optional<LogicalPlan> insertTarget(LogicalPlan command) {
    return invokePlanMethod(command, "logicalRelation");
  }

  /** Returns the source query of an {@code InsertIntoHoodieTableCommand}. */
  public static Optional<LogicalPlan> insertQuery(LogicalPlan command) {
    return invokePlanMethod(command, "query");
  }

  /** Checks whether an {@code InsertIntoHoodieTableCommand} is an {@code INSERT OVERWRITE}. */
  public static boolean isInsertOverwrite(LogicalPlan command) {
    try {
      return Boolean.TRUE.equals(MethodUtils.invokeMethod(command, "overwrite"));
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.warn("Unable to call overwrite on Hudi command {}", command.getClass().getName(), e);
      return false;
    }
  }

  private static Optional<LogicalPlan> invokePlanMethod(LogicalPlan command, String method) {
    try {
      Object result = MethodUtils.invokeMethod(command, method);
      return result instanceof LogicalPlan ? Optional.of((LogicalPlan) result) : Optional.empty();
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.warn("Unable to call {} on Hudi command {}", method, command.getClass().getName(), e);
      return Optional.empty();
    }
  }

  public static boolean isInstanceOf(Object instance, String className) {
    try {
      return Class.forName(className).isInstance(instance);
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /** Returns the meta client of a {@code HoodieBaseRelation}. */
  public static Object metaClient(Object relation) {
    try {
      return MethodUtils.invokeMethod(relation, "metaClient");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi meta client", e);
    }
  }

  /** Resolves the base path of a {@code HoodieBaseRelation}. */
  public static URI basePath(Object relation) {
    return basePath(relation, metaClient(relation));
  }

  /**
   * Resolves the base path of a {@code HoodieBaseRelation} with an already resolved meta client.
   */
  public static URI basePath(Object relation, Object metaClient) {
    try {
      Path path = (Path) MethodUtils.invokeMethod(relation, "basePath");
      if (path != null) {
        return path.toUri();
      }
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.debug("Unable to resolve Hudi relation basePath via reflection", e);
    }

    try {
      Object storagePath = MethodUtils.invokeMethod(metaClient, "getBasePathV2");
      return (URI) MethodUtils.invokeMethod(storagePath, "toUri");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi base path", e);
    }
  }
}
