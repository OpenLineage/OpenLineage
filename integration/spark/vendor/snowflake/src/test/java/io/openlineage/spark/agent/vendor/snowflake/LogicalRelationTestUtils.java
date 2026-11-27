/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.lang.reflect.Constructor;
import java.util.List;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import scala.Option;

/** Creates a real LogicalRelation across the Spark 3 and 4 constructor signatures. */
public final class LogicalRelationTestUtils {
  private LogicalRelationTestUtils() {}

  public static LogicalRelation create(
      SnowflakeRelation relation, List<AttributeReference> attributes) {
    Object output = ScalaConversionUtils.fromList(attributes);
    for (Constructor<?> constructor : LogicalRelation.class.getConstructors()) {
      int parameterCount = constructor.getParameterCount();
      if (parameterCount != 4 && parameterCount != 5) continue;
      try {
        return (LogicalRelation)
            (parameterCount == 4
                ? constructor.newInstance(relation, output, Option.empty(), false)
                : constructor.newInstance(relation, output, Option.empty(), false, Option.empty()));
      } catch (ReflectiveOperationException error) {
        throw new IllegalStateException("Could not create Spark LogicalRelation", error);
      }
    }
    throw new IllegalStateException("Unsupported Spark LogicalRelation constructor");
  }
}
