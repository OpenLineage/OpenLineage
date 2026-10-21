/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.package$;
import org.apache.spark.scheduler.SparkListenerEvent;

@Slf4j
public class ColumnLevelLineageUtils {

  /**
   * Column level lineage is supported only for Spark 3 and is disabled by default.
   *
   * @return
   */
  public static Optional<OpenLineage.ColumnLineageDatasetFacet> buildColumnLineageDatasetFacet(
      SparkListenerEvent event,
      OpenLineageContext context,
      OpenLineage.SchemaDatasetFacet schemaFacet) {
    return invokeSpark3("buildColumnLineageDatasetFacet", event, context, schemaFacet);
  }

  /**
   * Captures column lineage of a checkpointed plan, see {@link
   * io.openlineage.spark.api.Checkpoint}. Supported only for Spark 3.
   */
  public static Optional<CheckpointColumnLineage> buildCheckpointColumnLineage(
      SparkListenerEvent event,
      OpenLineageContext context,
      OpenLineage.SchemaDatasetFacet schemaFacet) {
    return invokeSpark3("buildCheckpointColumnLineage", event, context, schemaFacet);
  }

  @SuppressWarnings("unchecked")
  private static <T> Optional<T> invokeSpark3(
      String methodName,
      SparkListenerEvent event,
      OpenLineageContext context,
      OpenLineage.SchemaDatasetFacet schemaFacet) {
    // if Spark3 include column lineage
    if (!package$.MODULE$.SPARK_VERSION().startsWith("2")) {
      try {
        return (Optional<T>)
            Class.forName(
                    "io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils")
                .getMethod(
                    methodName,
                    SparkListenerEvent.class,
                    OpenLineageContext.class,
                    OpenLineage.SchemaDatasetFacet.class)
                .invoke(null, event, context, schemaFacet);
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException e) {
        log.error("Error when invoking static method '{}' for Spark3", methodName, e);
        return Optional.empty();
      } catch (RuntimeException e) {
        log.error("Error when building column level lineage", e);
        return Optional.empty();
      }
    }
    return Optional.empty();
  }
}
