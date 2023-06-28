/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.package$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@Slf4j
public class CustomCollectorsUtils {

  private static Stream<CustomColumnLineageVisitor> loadCollectors(OpenLineageContext context) {
    return Stream.concat(loadCustomCollectors(), loadSparkVersionCollectors(context));
  }

  private static Stream<CustomColumnLineageVisitor> loadCustomCollectors() {
    ServiceLoader<CustomColumnLineageVisitor> loader =
        ServiceLoader.load(CustomColumnLineageVisitor.class);

    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(
            loader.iterator(), Spliterator.IMMUTABLE & Spliterator.DISTINCT),
        false);
  }

  /**
   * Loads collectors attached to specific Spark version
   *
   * @return
   */
  private static Stream<CustomColumnLineageVisitor> loadSparkVersionCollectors(
      OpenLineageContext context) {
    String sparkVersion = package$.MODULE$.SPARK_VERSION().replace(".", "").substring(0, 2);

    String loader =
        String.format(
            "io.openlineage.spark%s.agent.lifecycle.plan.column.VisitorLoader", sparkVersion);

    try {
      Class<?> aClass = Class.forName(loader);
      return (Stream<CustomColumnLineageVisitor>)
          MethodUtils.invokeStaticMethod(aClass, "load", context);
    } catch (ClassNotFoundException e) {
      // no warn log
      return Stream.empty();
    } catch (Exception e) {
      log.warn("Failed loading custom loaders: ", e);
      return Stream.empty();
    }
  }

  private static Class getClass(String className, String packageName) {
    try {
      return Class.forName(packageName + "." + className.substring(0, className.lastIndexOf('.')));
    } catch (ClassNotFoundException e) {
      // handle the exception
    }
    return null;
  }

  static void collectInputs(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCollectors(context)
        .forEach(collector -> collector.collectInputs(plan, builder));
  }

  static void collectOutputs(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCollectors(context)
        .forEach(collector -> collector.collectOutputs(plan, builder));
  }

  static void collectExpressionDependencies(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCollectors(context)
        .forEach(collector -> collector.collectExpressionDependencies(plan, builder));
  }
}
