/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/**
 * Reflective accessors for {@code V2CreateTablePlan} nodes: {@code CreateTableAsSelect}, {@code
 * ReplaceTableAsSelect}, {@code CreateTable} and {@code ReplaceTable}.
 *
 * <p>Databricks runtimes ship a catalyst whose signatures for these nodes differ from the Apache
 * Spark release the runtime is based on - {@code tableSpec()} returning {@code TableSpec} instead
 * of {@code TableSpecBase} is the best known example. Calling such a member through the compile
 * time API throws {@link NoSuchMethodError}, which the builder framework swallows, so the event is
 * still emitted but without any output dataset. Resolving the members reflectively, with fallbacks
 * for the shapes seen across runtimes, keeps output extraction working on those platforms.
 */
@Slf4j
public class V2CreateTablePlanUtils {

  private V2CreateTablePlanUtils() {}

  /**
   * Resolves the catalog the table is created in. Since Spark 3.3 the catalog is reachable through
   * the {@code ResolvedIdentifier} held in {@code name()}; older plans expose {@code catalog()}
   * directly.
   */
  public static Optional<TableCatalog> catalog(LogicalPlan plan) {
    return firstOf(
        () ->
            cast(invoke(plan, "name").flatMap(name -> invoke(name, "catalog")), TableCatalog.class),
        () -> cast(invoke(plan, "catalog"), TableCatalog.class));
  }

  /** Resolves the identifier of the table being created or replaced. */
  public static Optional<Identifier> identifier(LogicalPlan plan) {
    return firstOf(
        () -> cast(invoke(plan, "tableName"), Identifier.class),
        () ->
            cast(
                invoke(plan, "name").flatMap(name -> invoke(name, "identifier")),
                Identifier.class));
  }

  /**
   * Table properties declared by the command, with the write options merged on top of them. Both
   * are used to resolve the dataset location, so a missing one must not prevent the other from
   * being read.
   */
  public static Map<String, String> properties(LogicalPlan plan) {
    Map<String, String> properties = new LinkedHashMap<>();
    Optional<Object> specProperties =
        invoke(plan, "tableSpec").flatMap(spec -> invoke(spec, "properties"));

    if (specProperties.isPresent()) {
      properties.putAll(toJavaMap(specProperties.get()));
    } else {
      invoke(plan, "properties").ifPresent(p -> properties.putAll(toJavaMap(p)));
    }

    invoke(plan, "writeOptions").ifPresent(options -> properties.putAll(toJavaMap(options)));
    return properties;
  }

  /**
   * Schema of the table being created. Falls back to the schema of the select query for plans that
   * do not expose {@code tableSchema()}, and to an empty schema when neither is available.
   */
  public static StructType schema(LogicalPlan plan) {
    return firstOf(
            () -> cast(invoke(plan, "tableSchema"), StructType.class),
            () ->
                cast(
                    invoke(plan, "query").flatMap(query -> invoke(query, "schema")),
                    StructType.class))
        .orElseGet(StructType::new);
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

  private static <T> Optional<T> cast(Optional<Object> value, Class<T> type) {
    return value.filter(type::isInstance).map(type::cast);
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

  @SuppressWarnings("unchecked")
  private static Map<String, String> toJavaMap(Object map) {
    if (map instanceof scala.collection.immutable.Map) {
      return ScalaConversionUtils.<String, String>fromMap(
          (scala.collection.immutable.Map<String, String>) map);
    } else if (map instanceof Map) {
      return new HashMap<>((Map<String, String>) map);
    }
    log.debug("Unsupported properties type {}", map.getClass().getCanonicalName());
    return new HashMap<>();
  }
}
