/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import scala.Option;

/**
 * Recognises the Databricks Delta commands that a {@code COPY INTO} statement is turned into, and
 * reads their target relation and source location.
 *
 * <p>On newer Databricks runtimes (Spark 4.0+) the logical plan is {@code CopyIntoCommandEdge}
 * rather than {@code CopyIntoCommand}. Neither class is on the compile classpath, so builders match
 * by canonical class name suffix and reach into the command reflectively, with fallbacks for the
 * member names and shapes seen across runtimes. When the command is not the optimized-plan root, or
 * reflection still fails, SQL text from the logical plan origin is parsed as a last resort.
 */
@Slf4j
public class CopyIntoCommandUtils {

  private static final List<String> COMMAND_CLASS_SUFFIXES =
      Arrays.asList(
          "sql.transaction.tahoe.commands.CopyIntoCommand",
          "sql.transaction.tahoe.commands.CopyIntoCommandEdge");

  private static final List<String> TARGET_METHOD_NAMES =
      Arrays.asList("target", "targetTable", "table", "copyIntoTarget", "copyIntoTable");

  private static final List<String> TARGET_FIELD_NAMES =
      Arrays.asList("target", "targetTable", "table", "copyIntoTarget", "copyIntoTable");

  private static final List<String> SOURCE_PATH_METHOD_NAMES =
      Arrays.asList(
          "sourcePath",
          "path",
          "sourceLocation",
          "location",
          "sourceUri",
          "inputPath",
          "copyIntoSourcePath",
          "from");

  private static final List<String> SOURCE_PATH_FIELD_NAMES =
      Arrays.asList(
          "sourcePath",
          "path",
          "sourceLocation",
          "location",
          "sourceUri",
          "inputPath",
          "copyIntoSourcePath",
          "from");

  private static final String SOURCE_MEMBER = "source";

  private static final List<String> SOURCE_QUERY_METHOD_NAMES =
      Arrays.asList("query", SOURCE_MEMBER, "copyIntoSource", "sourceQuery");

  private static final List<String> SOURCE_QUERY_FIELD_NAMES =
      Arrays.asList("query", SOURCE_MEMBER, "copyIntoSource", "sourceQuery");

  private static final List<String> NESTED_SOURCE_MEMBERS =
      Arrays.asList(
          "sourceOptions", "copyIntoOptions", "options", "readerOptions", "formatOptions");

  private CopyIntoCommandUtils() {}

  public static boolean isCopyIntoCommand(LogicalPlan plan) {
    return matchesCommandClassName(plan.getClass().getCanonicalName());
  }

  /** Package-visible for tests; Databricks command classes are not on the classpath. */
  static boolean matchesCommandClassName(String canonicalName) {
    if (canonicalName == null) {
      return false;
    }
    if (COMMAND_CLASS_SUFFIXES.stream().anyMatch(canonicalName::endsWith)) {
      return true;
    }
    return canonicalName.contains("CopyInto")
        && (canonicalName.endsWith("CommandEdge")
            || canonicalName.endsWith("Command")
            || canonicalName.endsWith("CopyIntoCommandEdge")
            || canonicalName.endsWith("CopyIntoCommand"));
  }

  /** Finds every COPY INTO command node across optimized, analyzed, and logical plans. */
  public static List<LogicalPlan> findAllCommands(OpenLineageContext context) {
    return context
        .getQueryExecution()
        .map(CopyIntoCommandUtils::findAllCommands)
        .orElse(Collections.emptyList());
  }

  private static List<LogicalPlan> findAllCommands(QueryExecution queryExecution) {
    Set<LogicalPlan> commands = new LinkedHashSet<>();
    findInTree(queryExecution.optimizedPlan()).ifPresent(commands::add);
    findInTree(queryExecution.analyzed()).ifPresent(commands::add);
    findInTree(queryExecution.logical()).ifPresent(commands::add);
    return new ArrayList<>(commands);
  }

  public static Optional<LogicalPlan> findInTree(LogicalPlan root) {
    if (root == null) {
      return Optional.empty();
    }
    java.util.ArrayDeque<LogicalPlan> stack = new java.util.ArrayDeque<>();
    stack.push(root);
    while (!stack.isEmpty()) {
      LogicalPlan plan = stack.pop();
      if (isCopyIntoCommand(plan)) {
        return Optional.of(plan);
      }
      ScalaConversionUtils.fromSeq(plan.children()).forEach(stack::push);
    }
    return Optional.empty();
  }

  /** Finds a table scan relation under a COPY INTO command for delegate-based extraction. */
  public static Optional<LogicalPlan> findTableRelation(LogicalPlan root) {
    Optional<LogicalPlan> dataSourceV2Relation = findDataSourceV2Relation(root);
    if (dataSourceV2Relation.isPresent()) {
      return dataSourceV2Relation;
    }
    return findLogicalRelation(root);
  }

  /**
   * Finds a {@link DataSourceV2Relation} under a COPY INTO command for delegate-based extraction.
   */
  public static Optional<LogicalPlan> findDataSourceV2Relation(LogicalPlan root) {
    return findInTreeByType(root, DataSourceV2Relation.class);
  }

  private static Optional<LogicalPlan> findLogicalRelation(LogicalPlan root) {
    return findInTreeByType(root, LogicalRelation.class);
  }

  private static Optional<LogicalPlan> findInTreeByType(LogicalPlan root, Class<?> type) {
    if (root == null) {
      return Optional.empty();
    }
    java.util.ArrayDeque<LogicalPlan> stack = new java.util.ArrayDeque<>();
    stack.push(root);
    while (!stack.isEmpty()) {
      LogicalPlan plan = stack.pop();
      if (type.isInstance(plan)) {
        return Optional.of(plan);
      }
      ScalaConversionUtils.fromSeq(plan.children()).forEach(stack::push);
    }
    return Optional.empty();
  }

  public static Optional<String> sqlText(OpenLineageContext context) {
    return context
        .getQueryExecution()
        .flatMap(
            qe ->
                firstOf(
                    () -> sqlFromPlanTree(qe.logical()),
                    () -> sqlFromPlanTree(qe.analyzed()),
                    () -> sqlFromPlanTree(qe.optimizedPlan())))
        .filter(CopyIntoSqlUtils::isCopyIntoStatement);
  }

  private static Optional<String> sqlFromPlanTree(LogicalPlan root) {
    if (root == null) {
      return Optional.empty();
    }
    java.util.ArrayDeque<LogicalPlan> stack = new java.util.ArrayDeque<>();
    stack.push(root);
    while (!stack.isEmpty()) {
      LogicalPlan plan = stack.pop();
      Optional<String> sql = sqlFromOrigin(plan);
      if (sql.isPresent()) {
        return sql;
      }
      ScalaConversionUtils.fromSeq(plan.children()).forEach(stack::push);
    }
    return Optional.empty();
  }

  private static Optional<String> sqlFromOrigin(LogicalPlan plan) {
    if (plan.origin() == null) {
      return Optional.empty();
    }
    Object origin = plan.origin();
    return firstOf(
        () -> stringFromOriginMember(invoke(origin, "sqlText")),
        () -> stringFromOriginMember(invoke(origin, "sql")),
        () -> stringFromOriginMember(readField(origin, "sqlText")),
        () -> stringFromOriginMember(readField(origin, "sql")));
  }

  private static Optional<String> stringFromOriginMember(Optional<Object> value) {
    if (value.isEmpty()) {
      return Optional.empty();
    }
    Object object = value.get();
    if (object instanceof Option) {
      Option<?> option = (Option<?>) object;
      if (option.isDefined() && option.get() instanceof String) {
        return Optional.of((String) option.get()).filter(StringUtils::isNotBlank);
      }
      return Optional.empty();
    }
    return stringValue(Optional.of(object));
  }

  /**
   * Reads the target relation, unwrapping the {@link SubqueryAlias} the command keeps when the
   * statement uses a table alias.
   */
  public static Optional<LogicalPlan> target(LogicalPlan plan) {
    return targetFromCommand(plan);
  }

  /**
   * Package-visible so tests can pass a stand-in for the Databricks command, which cannot extend
   * {@link LogicalPlan} from Java.
   */
  static Optional<LogicalPlan> targetFromCommand(Object command) {
    Optional<LogicalPlan> target =
        firstOf(
            () -> logicalPlanFromMethods(command, TARGET_METHOD_NAMES),
            () -> logicalPlanFromFields(command, TARGET_FIELD_NAMES),
            () -> targetFromAnyMember(command));

    return target.map(CopyIntoCommandUtils::unwrapSubqueryAlias);
  }

  /**
   * Picks the target out of the runtime-only members when none of the known accessor names match.
   * Databricks renames these members between runtimes, and a miss costs the whole output dataset:
   * the builder then falls back to the table name alone, which is a different lineage node than the
   * storage location that DELETE and CTAS resolve for the same table.
   */
  private static Optional<LogicalPlan> targetFromAnyMember(Object command) {
    // Only a catalog-backed relation is accepted. Falling back to "any plan member" would let the
    // source scan be reported as the table the statement wrote to, which is worse than degrading to
    // the SQL text.
    return logicalPlanMembers(command).stream()
        .filter(plan -> findCatalogRelation(plan).isPresent())
        .findFirst();
  }

  /**
   * Reads every {@link LogicalPlan} held by the command, in declaration order. Only fields are
   * read: the command also declares methods such as {@code run}, and invoking those to discover a
   * member would re-execute the statement.
   */
  private static List<LogicalPlan> logicalPlanMembers(Object command) {
    List<LogicalPlan> plans = new ArrayList<>();
    for (java.lang.reflect.Field field : runtimeFields(command)) {
      readField(command, field).flatMap(CopyIntoCommandUtils::asLogicalPlan).ifPresent(plans::add);
    }
    return plans;
  }

  /**
   * Fields declared by the command itself and its runtime-specific supertypes. Spark and Scala base
   * classes are skipped: their members are tree bookkeeping, never the COPY INTO operands.
   */
  private static List<java.lang.reflect.Field> runtimeFields(Object command) {
    List<java.lang.reflect.Field> fields = new ArrayList<>();
    Class<?> type = command.getClass();
    while (type != null && isRuntimeClass(type)) {
      fields.addAll(Arrays.asList(type.getDeclaredFields()));
      type = type.getSuperclass();
    }
    return fields;
  }

  private static boolean isRuntimeClass(Class<?> type) {
    String name = type.getName();
    return !name.startsWith("org.apache.spark.")
        && !name.startsWith("scala.")
        && !name.startsWith("java.");
  }

  private static Optional<LogicalPlan> asLogicalPlan(Object value) {
    if (value instanceof LogicalPlan) {
      return Optional.of((LogicalPlan) value);
    }
    if (value instanceof Option) {
      Option<?> option = (Option<?>) value;
      return option.isDefined() ? asLogicalPlan(option.get()) : Optional.empty();
    }
    return Optional.empty();
  }

  /** A relation that a catalog can resolve, which is what distinguishes the target from a scan. */
  private static Optional<DataSourceV2Relation> findCatalogRelation(LogicalPlan plan) {
    return findInTreeByType(plan, DataSourceV2Relation.class)
        .map(DataSourceV2Relation.class::cast)
        .filter(relation -> relation.identifier() != null && relation.identifier().isDefined());
  }

  private static Optional<Object> readField(Object target, java.lang.reflect.Field field) {
    try {
      return Optional.ofNullable(FieldUtils.readField(field, target, true));
    } catch (IllegalAccessException | RuntimeException e) {
      log.debug("Could not read {} from {}", field.getName(), target.getClass().getName(), e);
      return Optional.empty();
    }
  }

  /**
   * Lists the zero-argument methods and fields declared by a runtime-only command class. The
   * Databricks classes are not on the compile classpath, so when every known accessor name misses
   * this is the only way to learn the real member names from a driver log.
   */
  public static String describeMembers(Object plan) {
    if (plan == null) {
      return "<null>";
    }
    Class<?> type = plan.getClass();
    String methods =
        Arrays.stream(type.getDeclaredMethods())
            .filter(method -> method.getParameterCount() == 0)
            .map(method -> method.getName() + ":" + method.getReturnType().getSimpleName())
            .sorted()
            .collect(Collectors.joining(", "));
    String fields =
        Arrays.stream(type.getDeclaredFields())
            .map(field -> field.getName() + ":" + field.getType().getSimpleName())
            .sorted()
            .collect(Collectors.joining(", "));
    return type.getCanonicalName() + " methods=[" + methods + "] fields=[" + fields + "]";
  }

  /** Reads the source file path when the command stores it as a string or in reader options. */
  public static Optional<String> sourcePath(LogicalPlan plan) {
    return sourcePathFromCommand(plan);
  }

  /** Package-visible for tests; see {@link #targetFromCommand(Object)}. */
  static Optional<String> sourcePathFromCommand(Object command) {
    return firstOf(
        () -> stringFromMethods(command, SOURCE_PATH_METHOD_NAMES),
        () -> stringFromFields(command, SOURCE_PATH_FIELD_NAMES),
        () -> pathFromNestedMembers(command),
        () -> pathFromOptions(invoke(command, "sourceOptions")),
        () -> pathFromOptions(readField(command, "sourceOptions")),
        () -> stringFromSourceMember(command),
        () -> pathFromAnyMember(command));
  }

  /**
   * Picks the source location out of the runtime-only members when none of the known accessor names
   * match. Only values shaped like a location are considered, so sibling members such as the file
   * format are not mistaken for a path.
   */
  private static Optional<String> pathFromAnyMember(Object command) {
    for (java.lang.reflect.Field field : runtimeFields(command)) {
      Optional<String> path =
          readField(command, field)
              .flatMap(value -> stringValue(Optional.of(value)))
              .filter(CopyIntoCommandUtils::looksLikeLocation);
      if (path.isPresent()) {
        return path;
      }
    }
    return Optional.empty();
  }

  private static boolean looksLikeLocation(String value) {
    return value.startsWith("/") || value.contains("://");
  }

  /** Reads a nested select over the source when the command does not expose a plain path. */
  public static Optional<LogicalPlan> sourceQuery(LogicalPlan plan) {
    return sourceQueryFromCommand(plan);
  }

  /** Package-visible for tests; see {@link #targetFromCommand(Object)}. */
  static Optional<LogicalPlan> sourceQueryFromCommand(Object command) {
    return firstOf(
        () -> logicalPlanFromMethods(command, SOURCE_QUERY_METHOD_NAMES),
        () -> logicalPlanFromFields(command, SOURCE_QUERY_FIELD_NAMES),
        () -> sourceQueryFromAnyMember(command));
  }

  /** The plan member that is not the target, so a source select is not reported as the output. */
  private static Optional<LogicalPlan> sourceQueryFromAnyMember(Object command) {
    Optional<LogicalPlan> target = targetFromAnyMember(command);
    return logicalPlanMembers(command).stream()
        .filter(plan -> !target.filter(t -> t == plan).isPresent())
        .findFirst();
  }

  private static Optional<String> pathFromNestedMembers(Object plan) {
    for (String member : NESTED_SOURCE_MEMBERS) {
      Optional<String> path =
          firstOf(
              () -> pathFromOptions(invoke(plan, member)),
              () -> pathFromOptions(readField(plan, member)),
              () -> stringValue(invoke(plan, member)),
              () -> stringValue(readField(plan, member)));
      if (path.isPresent()) {
        return path;
      }
    }
    return Optional.empty();
  }

  private static Optional<LogicalPlan> logicalPlanFromMethods(
      Object plan, List<String> methodNames) {
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

  private static Optional<String> stringFromSourceMember(Object plan) {
    return stringValue(
        firstOf(() -> invoke(plan, SOURCE_MEMBER), () -> readField(plan, SOURCE_MEMBER)));
  }

  private static Optional<String> pathFromOptions(Optional<Object> options) {
    return options.flatMap(CopyIntoCommandUtils::pathFromOptionsMap);
  }

  private static Optional<String> pathFromOptionsMap(Object options) {
    Map<String, String> optionMap = toJavaMap(options);
    return firstOf(
        () -> Optional.ofNullable(optionMap.get("path")).filter(StringUtils::isNotBlank),
        () -> Optional.ofNullable(optionMap.get("location")).filter(StringUtils::isNotBlank),
        () -> Optional.ofNullable(optionMap.get("uri")).filter(StringUtils::isNotBlank));
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
        () -> logicalPlan(invoke(object, "plan")),
        () -> logicalPlan(invoke(object, "target")));
  }

  private static Optional<String> stringValue(Optional<Object> value) {
    return value
        .filter(String.class::isInstance)
        .map(String.class::cast)
        .filter(StringUtils::isNotBlank);
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
