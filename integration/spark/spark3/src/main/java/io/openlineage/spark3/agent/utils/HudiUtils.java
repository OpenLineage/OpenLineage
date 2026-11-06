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
import org.apache.spark.sql.types.StructType;

/**
 * Reflection based helpers for Hudi relations. Hudi classes are not guaranteed to be present on the
 * classpath, so they are accessed by name only.
 */
@Slf4j
public class HudiUtils {
  private static final String HOODIE_BASE_RELATION_CLASS = "org.apache.hudi.HoodieBaseRelation";
  private static final String HOODIE_FILE_INDEX_CLASS = "org.apache.hudi.HoodieFileIndex";
  private static final String INSERT_INTO_HOODIE_TABLE_COMMAND_CLASS =
      "org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand";

  private HudiUtils() {}

  /** Checks whether the relation is a Hudi {@code HoodieBaseRelation} (e.g. MOR or incremental). */
  public static boolean isHudiBaseRelation(Object relation) {
    return isInstanceOf(relation, HOODIE_BASE_RELATION_CLASS);
  }

  /**
   * Checks whether the file index of a {@code HadoopFsRelation} is Hudi's {@code HoodieFileIndex}.
   */
  public static boolean isHudiFileIndex(Object fileIndex) {
    return isInstanceOf(fileIndex, HOODIE_FILE_INDEX_CLASS);
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

  /** Returns the meta client of a {@code HoodieBaseRelation} or a {@code HoodieFileIndex}. */
  public static Object metaClient(Object relationOrFileIndex) {
    try {
      return MethodUtils.invokeMethod(relationOrFileIndex, "metaClient");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi meta client", e);
    }
  }

  /** Returns the schema of a {@code HoodieBaseRelation}. */
  public static StructType schema(Object relation) {
    try {
      return (StructType) MethodUtils.invokeMethod(relation, "schema");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi schema", e);
    }
  }

  /** Returns the table type ({@code COPY_ON_WRITE} or {@code MERGE_ON_READ}) of a meta client. */
  public static String tableType(Object metaClient) {
    try {
      return MethodUtils.invokeMethod(metaClient, "getTableType").toString();
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi table type", e);
    }
  }

  /**
   * Resolves the base path of a {@code HoodieBaseRelation}, or returns empty if it can't be
   * resolved. The meta client is only used when the relation has no {@code basePath()} of its own.
   */
  public static Optional<URI> basePath(Object relation) {
    Optional<URI> uri = pathUri(relation, "basePath");
    if (uri.isPresent()) {
      return uri;
    }
    try {
      return metaClientBasePath(metaClient(relation));
    } catch (RuntimeException e) {
      log.debug("Unable to resolve Hudi base path", e);
      return Optional.empty();
    }
  }

  /**
   * Resolves the base path of a {@code HoodieBaseRelation} with an already resolved meta client.
   * Hudi 0.x returns a Hadoop {@code Path} or a {@code String}, Hudi 1.x returns a {@code
   * StoragePath}, so any value with a {@code toUri()} method is accepted.
   */
  public static URI basePath(Object relation, Object metaClient) {
    Optional<URI> uri = pathUri(relation, "basePath");
    if (!uri.isPresent()) {
      uri = metaClientBasePath(metaClient);
    }
    return uri.orElseThrow(() -> new IllegalStateException("Unable to resolve Hudi base path"));
  }

  private static Optional<URI> metaClientBasePath(Object metaClient) {
    Optional<URI> uri = pathUri(metaClient, "getBasePath");
    return uri.isPresent() ? uri : pathUri(metaClient, "getBasePathV2");
  }

  private static Optional<URI> pathUri(Object target, String method) {
    if (target == null) {
      return Optional.empty();
    }
    try {
      Object path = MethodUtils.invokeMethod(target, method);
      if (path instanceof String) {
        return Optional.of(new Path((String) path).toUri());
      }
      if (path != null) {
        Object uri = MethodUtils.invokeMethod(path, "toUri");
        if (uri instanceof URI) {
          return Optional.of((URI) uri);
        }
      }
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.debug("Unable to resolve Hudi base path via {}", method, e);
    }
    return Optional.empty();
  }
}
