/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructType;
import scala.Option;

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
  private static final String MERGE_INTO_HOODIE_TABLE_COMMAND_CLASS =
      "org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand";
  private static final String UPDATE_HOODIE_TABLE_COMMAND_CLASS =
      "org.apache.spark.sql.hudi.command.UpdateHoodieTableCommand";
  private static final String DELETE_HOODIE_TABLE_COMMAND_CLASS =
      "org.apache.spark.sql.hudi.command.DeleteHoodieTableCommand";
  private static final String CREATE_HOODIE_TABLE_AS_SELECT_COMMAND_CLASS =
      "org.apache.spark.sql.hudi.command.CreateHoodieTableAsSelectCommand";

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
   * {@code INSERT INTO/OVERWRITE} on Hudi tables. Its target is never a child of the command, and
   * its query is a child only from Hudi 1.x (a {@code DataWritingCommand}; in 0.x it is a leaf).
   */
  public static boolean isInsertIntoHoodieTableCommand(LogicalPlan plan) {
    return isInstanceOf(plan, INSERT_INTO_HOODIE_TABLE_COMMAND_CLASS);
  }

  /**
   * Checks whether the plan is one of the commands Hudi SQL uses for {@code MERGE INTO}, {@code
   * UPDATE} and {@code DELETE} on Hudi tables. Their target is never a child of the command; in
   * Hudi 1.x, {@code UPDATE} and {@code DELETE} have a query reading the target as a child.
   */
  public static boolean isHudiDmlCommand(LogicalPlan plan) {
    return isMergeIntoHoodieTableCommand(plan)
        || isInstanceOf(plan, UPDATE_HOODIE_TABLE_COMMAND_CLASS)
        || isInstanceOf(plan, DELETE_HOODIE_TABLE_COMMAND_CLASS);
  }

  /** Checks whether the plan is Hudi's {@code MergeIntoHoodieTableCommand} ({@code MERGE INTO}). */
  public static boolean isMergeIntoHoodieTableCommand(LogicalPlan plan) {
    return isInstanceOf(plan, MERGE_INTO_HOODIE_TABLE_COMMAND_CLASS);
  }

  /**
   * Checks whether the plan is Hudi's {@code CreateHoodieTableAsSelectCommand} ({@code CREATE TABLE
   * ... AS SELECT} of a Hudi table). Its query is a child only from Hudi 1.x.
   */
  public static boolean isCreateHoodieTableAsSelectCommand(LogicalPlan plan) {
    return isInstanceOf(plan, CREATE_HOODIE_TABLE_AS_SELECT_COMMAND_CLASS);
  }

  /**
   * Returns the relation of the target table of a Hudi {@code MERGE INTO}, {@code UPDATE} or {@code
   * DELETE} command. Hudi 1.x's {@code DeleteHoodieTableCommand} only keeps the query that reads
   * the target, so the relation is taken from there.
   */
  public static Optional<LogicalRelation> dmlTarget(LogicalPlan command) {
    Optional<LogicalPlan> table;
    if (isMergeIntoHoodieTableCommand(command)) {
      table = mergeInto(command).map(MergeIntoTable::targetTable);
    } else if (isInstanceOf(command, UPDATE_HOODIE_TABLE_COMMAND_CLASS)) {
      table =
          invoke(command, "ut")
              .filter(UpdateTable.class::isInstance)
              .map(update -> ((UpdateTable) update).table());
    } else if (isInstanceOf(command, DELETE_HOODIE_TABLE_COMMAND_CLASS)) {
      Optional<LogicalPlan> deleteFrom =
          invoke(command, "dft")
              .filter(DeleteFromTable.class::isInstance)
              .map(delete -> ((DeleteFromTable) delete).table());
      table = deleteFrom.isPresent() ? deleteFrom : invokePlanMethod(command, "query");
    } else {
      table = Optional.empty();
    }
    return table.flatMap(
        plan ->
            ScalaConversionUtils.fromSeq(plan.collectLeaves()).stream()
                .filter(LogicalRelation.class::isInstance)
                .map(LogicalRelation.class::cast)
                .findFirst());
  }

  /** Returns the Spark {@code MergeIntoTable} a {@code MergeIntoHoodieTableCommand} runs. */
  public static Optional<MergeIntoTable> mergeInto(LogicalPlan command) {
    return invoke(command, "mergeInto")
        .filter(MergeIntoTable.class::isInstance)
        .map(MergeIntoTable.class::cast);
  }

  /** Returns the table created by a {@code CreateHoodieTableAsSelectCommand}. */
  public static Optional<CatalogTable> ctasTable(LogicalPlan command) {
    return invoke(command, "table")
        .filter(CatalogTable.class::isInstance)
        .map(CatalogTable.class::cast);
  }

  /** Returns the query of a {@code CreateHoodieTableAsSelectCommand}. */
  public static Optional<LogicalPlan> ctasQuery(LogicalPlan command) {
    return invokePlanMethod(command, "query");
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

  /**
   * Returns the static partition columns of an {@code InsertIntoHoodieTableCommand}: those given a
   * value in its {@code PARTITION} clause, which the query doesn't produce.
   */
  public static Set<String> staticPartitionColumns(LogicalPlan command) {
    try {
      Object spec = MethodUtils.invokeMethod(command, "partitionSpec");
      if (!(spec instanceof scala.collection.immutable.Map)) {
        return Collections.emptySet();
      }
      return ScalaConversionUtils.<String, Option<String>>fromMap(
              (scala.collection.immutable.Map<String, Option<String>>) spec)
          .entrySet()
          .stream()
          .filter(e -> e.getValue() != null && e.getValue().isDefined())
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.warn("Unable to call partitionSpec on Hudi command {}", command.getClass().getName(), e);
      return Collections.emptySet();
    }
  }

  /** Calls a method that the command has only in some Hudi versions. */
  private static Optional<Object> invoke(LogicalPlan command, String method) {
    try {
      return Optional.ofNullable(MethodUtils.invokeMethod(command, method));
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.debug("Unable to call {} on Hudi command {}", method, command.getClass().getName(), e);
      return Optional.empty();
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

  /**
   * Checks whether {@code instance} is of the class {@code className} or a subclass of it, by name.
   * Hudi is often added per job (e.g. with {@code --packages}), so its classes may not be visible
   * from this class's classloader; comparing names also avoids a failed class lookup on every call
   * when Hudi is absent. Only classes are matched, not interfaces.
   */
  public static boolean isInstanceOf(Object instance, String className) {
    for (Class<?> c = instance == null ? null : instance.getClass();
        c != null;
        c = c.getSuperclass()) {
      if (c.getName().equals(className)) {
        return true;
      }
    }
    return false;
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

  /**
   * Returns the table type ({@code COPY_ON_WRITE} or {@code MERGE_ON_READ}) of a meta client, or
   * empty if it can't be read.
   */
  public static Optional<String> tableType(Object metaClient) {
    try {
      return Optional.ofNullable(MethodUtils.invokeMethod(metaClient, "getTableType"))
          .map(Object::toString);
    } catch (IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException
        | RuntimeException e) {
      log.debug("Unable to resolve Hudi table type", e);
      return Optional.empty();
    }
  }

  /**
   * Resolves the base path of a {@code HoodieBaseRelation}, or returns empty if it can't be
   * resolved. The meta client is only used when the relation has no {@code basePath()} of its own.
   */
  public static Optional<URI> basePath(Object relation) {
    try {
      return resolveBasePath(relation, () -> metaClient(relation));
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
    return resolveBasePath(relation, () -> metaClient)
        .orElseThrow(() -> new IllegalStateException("Unable to resolve Hudi base path"));
  }

  private static Optional<URI> resolveBasePath(Object relation, Supplier<Object> metaClient) {
    Optional<URI> uri = pathUri(relation, "basePath");
    return uri.isPresent() ? uri : metaClientBasePath(metaClient.get());
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
