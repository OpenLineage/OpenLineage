/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.MethodUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.execution.QueryExecution;

/** Utility class for common Spark methods and properties that are compatible with Spark 4.x. */
public class Spark4CompatUtils {

  @SneakyThrows
  public static void cleanupAnyExistingSession() {
    // check if SparkSession$.MODULE$.cleanupAnyExistingSession(); exists
    Method method =
        MethodUtils.getAccessibleMethod(
            SparkSession$.class, "cleanupAnyExistingSession", new Class[] {});
      // Spark 4.0 -> use classic spark session
      Class c = Class.forName("org.apache.spark.sql.classic.SparkSession$");
      Field field = c.getField("MODULE$");
      MethodUtils.invokeMethod(field.get(null), "cleanupAnyExistingSession", new Object[] {});

  }

  @SneakyThrows
  public static SparkSession getSparkSession(QueryExecution queryExecution) {
    return (SparkSession)
        org.apache.commons.lang3.reflect.MethodUtils.invokeMethod(queryExecution, "sparkSession");
  }

  /**
   * In Spark 4.x, the SparkSession builder should not call the enableHiveSupport method.
   *
   * @return
   */
  public static SparkSession.Builder builderWithHiveSupport() {
    boolean isSpark4 = System.getProperty("spark.version").startsWith("4");
    Builder builder = SparkSession.builder();
    if (!isSpark4) {
      builder.enableHiveSupport();
    }
    return builder;
  }
}
