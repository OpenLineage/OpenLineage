/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;

/**
 * Recognises the Databricks Delta commands that a {@code COPY INTO} statement is turned into, and
 * reads their target relation and source location.
 *
 * <p>On newer Databricks runtimes (Spark 4.0+) the logical plan is {@code CopyIntoCommandEdge}
 * rather than {@code CopyIntoCommand}. Neither class is on the compile classpath, so builders match
 * by canonical class name suffix and reach into the command reflectively, with fallbacks for the
 * member names and shapes seen across runtimes.
 */
@Slf4j
public class CopyIntoCommandUtils {

  private static final List<String> COMMAND_CLASS_NAMES =
      Arrays.asList(
          "sql.transaction.tahoe.commands.CopyIntoCommand",
          "sql.transaction.tahoe.commands.CopyIntoCommandEdge");

  private static final List<String> TARGET_METHOD_NAMES =
      Arrays.asList("target", "targetTable", "table");

  private static final List<String> TARGET_FIELD_NAMES =
      Arrays.asList("target", "targetTable", "table");

  private static final List<String> SOURCE_PATH_METHOD_NAMES =
      Arrays.asList("sourcePath", "path", "sourceLocation", "location");

  private static final List<String> SOURCE_PATH_FIELD_NAMES =
      Arrays.asList("sourcePath", "path", "sourceLocation", "location");

  private static final List<String> SOURCE_QUERY_METHOD_NAMES = Arrays.asList("query", "source");

  private static final List<String> SOURCE_QUERY_FIELD_NAMES = Arrays.asList("query", "source");

  private CopyIntoCommandUtils() {}

  public static boolean isCopyIntoCommand(LogicalPlan plan) {
    return matchesCommandClassName(plan.getClass().getCanonicalName());
  }

  /** Package-visible for tests; Databricks command classes are not on the classpath. */
  static boolean matchesCommandClassName(String canonicalName) {
    return canonicalName != null && COMMAND_CLASS_NAMES.stream().anyMatch(canonicalName::endsWith);
  }

  /**
   * Reads the target relation, unwrapping the {@link SubqueryAlias} the command keeps when the
   * statement uses a table alias.
   */
  public static Optional<LogicalPlan> target(LogicalPlan plan) {
    Optional<LogicalPlan> target =
        firstOf(
            () -> logicalPlanFromMethods(plan, TARGET_METHOD_NAMES),
            () -> logicalPlanFromFields(plan, TARGET_FIELD_NAMES));

    return target.map(CopyIntoCommandUtils::unwrapSubqueryAlias);
  }

  /** Reads the source file path when the command stores it as a string or in reader options. */
  public static Optional<String> sourcePath(LogicalPlan plan) {
    return firstOf(
        () -> stringFromMethods(plan, SOURCE_PATH_METHOD_NAMES),
        () -> stringFromFields(plan, SOURCE_PATH_FIELD_NAMES),
        () -> pathFromOptions(invoke(plan, "sourceOptions")),
        () -> pathFromOptions(readField(plan, "sourceOptions")),
        () -> stringFromSourceMember(plan));
  }

  /** Reads a nested select over the source when the command does not expose a plain path. */
  public static Optional<LogicalPlan> sourceQuery(LogicalPlan plan) {
    return firstOf(
        () -> logicalPlanFromMethods(plan, SOURCE_QUERY_METHOD_NAMES),
        () -> logicalPlanFromFields(plan, SOURCE_QUERY_FIELD_NAMES));
  }

  private static Optional<LogicalPlan> logicalPlanFromMethods(Object plan, List<String> methodNames) {
    for (String methodName : methodNames) {
      Optional<LogicalPlan> logicalPlan = logicalPlan(invoke(plan, methodName));
      if (logicalPlan.isPresent()) {
        return logicalPlan;
      }
    }
    return Optional.empty();
  }

  private static Optional<LogicalPlan> logicalPlanFromFields(Object plan, List<String> fieldNames) {
    for (String fieldName : fieldNames) {
      Optional<LogicalPlan> logicalPlan = logicalPlan(readField(plan, fieldName));
      if (logicalPlan.isPresent()) {
        return logicalPlan;
      }
    }
    return Optional.empty();
  }

  private static Optional<String> stringFromMethods(Object plan, List<String> methodNames) {
    for (String methodName : methodNames) {
      Optional<String> value = stringValue(invoke(plan, methodName));
      if (value.isPresent()) {
        return value;
      }
    }
    return Optional.empty();
  }

  private static Optional<String> stringFromFields(Object plan, List<String> fieldNames) {
    for (String fieldName : fieldNames) {
      Optional<String> value = stringValue(readField(plan, fieldName));
      if (value.isPresent()) {
        return value;
      }
    }
    return Optional.empty();
  }

  private static Optional<String> stringFromSourceMember(LogicalPlan plan) {
    return stringValue(firstOf(() -> invoke(plan, "source"), () -> readField(plan, "source")));
  }

  private static Optional<String> pathFromOptions(Optional<Object> options) {
    return options.flatMap(CopyIntoCommandUtils::pathFromOptionsMap);
  }

  private static Optional<String> pathFromOptionsMap(Object options) {
    Map<String, String> optionMap = toJavaMap(options);
    return Optional.ofNullable(optionMap.get("path")).filter(StringUtils::isNotBlank);
  }

  private static Optional<Object> invoke(Object target, String methodName) {
    if (target == null) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(MethodUtils.invokeMethod(target, methodName));
    } catch (Exception | NoSuchMethodError | NoClassDefFoundError e) {
      log.debug("Could not call {} on {}", methodName, target.getClass().getCanonicalName(), e);
      return Optional.empty();
    }
  }

  private static Optional<Object> readField(Object target, String fieldName) {
    if (target == null) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(FieldUtils.readField(target, fieldName, true));
    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.debug("Could not read {} from {}", fieldName, target.getClass().getCanonicalName(), e);
      return Optional.empty();
    }
  }

  private static Optional<LogicalPlan> logicalPlan(Optional<Object> value) {
    if (value.isEmpty()) {
      return Optional.empty();
    }
    Object object = value.get();
    if (object instanceof LogicalPlan) {
      return Optional.of((LogicalPlan) object);
    }
    return firstOf(
        () -> logicalPlan(invoke(object, "table")),
        () -> logicalPlan(invoke(object, "child")),
        () -> logicalPlan(invoke(object, "plan")));
  }

  private static Optional<String> stringValue(Optional<Object> value) {
    return value.filter(String.class::isInstance).map(String.class::cast).filter(StringUtils::isNotBlank);
  }

  private static LogicalPlan unwrapSubqueryAlias(LogicalPlan plan) {
    if (plan instanceof SubqueryAlias) {
      return ((SubqueryAlias) plan).child();
    }
    return plan;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> toJavaMap(Object map) {
    if (map instanceof scala.collection.immutable.Map) {
      return ScalaConversionUtils.fromMap((scala.collection.immutable.Map<String, String>) map);
    } else if (map instanceof Map) {
      return new HashMap<>((Map<String, String>) map);
    }
    return new HashMap<>();
  }

  @SafeVarargs
  private static <T> Optional<T> firstOf(Supplier<Optional<T>>... suppliers) {
    for (Supplier<Optional<T>> supplier : suppliers) {
      Optional<T> value = supplier.get();
      if (value.isPresent()) {
        return value;
      }
    }
    return Optional.empty();
  }
}
