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

    final int SPARK_3_PARAM_COUNT = 4;
    final int SPARK_4_PARAM_COUNT = 5;

    try {
      Constructor<?> constructor = getLogicalRelationConstructor();
      if (constructor.getParameterCount() == SPARK_3_PARAM_COUNT) {
        return Optional.of(
            (LogicalRelation)
                constructor.newInstance(relation, attributes, catalogTable, isStreaming));
      } else if (constructor.getParameterCount() == SPARK_4_PARAM_COUNT) {
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

      // Find constructor by parameter count
      for (Constructor<?> constructor : LOGICAL_RELATION_CLASS.getConstructors()) {
        int paramCount = constructor.getParameterCount();
        if (paramCount == 5 || paramCount == 4) {
          cachedConstructor = constructor;
          log.debug("Using {}-parameter LogicalRelation constructor", paramCount);
          return cachedConstructor;
        }
      }

      throw new NoSuchMethodException("No compatible LogicalRelation constructor found");
    }
  }
}
