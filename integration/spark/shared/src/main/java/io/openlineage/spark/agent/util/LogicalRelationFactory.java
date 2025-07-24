/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.lang.reflect.Constructor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import scala.Option;
import scala.collection.Seq;

@Slf4j
public class LogicalRelationFactory {

  private static final Class<?> LOGICAL_RELATION_CLASS = LogicalRelation.class;
  private static volatile Constructor<?> cachedConstructor;

  /**
   * Creates LogicalRelation using reflection to handle version differences between Spark 3.x and
   * 4.x. Spark 4.x added an additional stream parameter to the constructor.
   *
   * @param relation The BaseRelation
   * @param attributes Sequence of AttributeReference
   * @param catalogTable Optional CatalogTable
   * @param isStreaming Boolean indicating if this is a streaming relation
   * @return Optional LogicalRelation instance, empty if creation fails
   */
  public static Optional<LogicalRelation> create(
      BaseRelation relation,
      Seq<AttributeReference> attributes,
      Option<?> catalogTable,
      boolean isStreaming) {

    try {
      Constructor<?> constructor = getLogicalRelationConstructor();

      if (constructor.getParameterCount() == 4) {
        // Spark 3.x constructor
        return Optional.of(
            (LogicalRelation)
                constructor.newInstance(relation, attributes, catalogTable, isStreaming));
      } else if (constructor.getParameterCount() == 5) {
        // Spark 4.x constructor with additional stream parameter
        return Optional.of(
            (LogicalRelation)
                constructor.newInstance(
                    relation, attributes, catalogTable, isStreaming, Option.empty()));
      }

      log.warn(
          "Unexpected LogicalRelation constructor parameter count: {}",
          constructor.getParameterCount());
      return Optional.empty();

    } catch (Exception e) {
      log.warn("Failed to create LogicalRelation using reflection", e);
      return Optional.empty();
    }
  }

  private static Constructor<?> getLogicalRelationConstructor() throws Exception {
    if (cachedConstructor != null) {
      return cachedConstructor;
    }

    synchronized (LogicalRelationFactory.class) {
      if (cachedConstructor != null) {
        return cachedConstructor;
      }

      // Try Spark 4.x constructor first (5 parameters)
      try {
        cachedConstructor =
            LOGICAL_RELATION_CLASS.getConstructor(
                BaseRelation.class,
                Seq.class,
                Option.class,
                boolean.class,
                Option.class // stream parameter
                );
        log.debug("Using Spark 4.x LogicalRelation constructor (5 parameters)");
        return cachedConstructor;
      } catch (NoSuchMethodException ignored) {
        // Fall back to Spark 3.x constructor
      }

      // Try Spark 3.x constructor (4 parameters)
      cachedConstructor =
          LOGICAL_RELATION_CLASS.getConstructor(
              BaseRelation.class, Seq.class, Option.class, boolean.class);
      log.debug("Using Spark 3.x LogicalRelation constructor (4 parameters)");

      return cachedConstructor;
    }
  }
}
