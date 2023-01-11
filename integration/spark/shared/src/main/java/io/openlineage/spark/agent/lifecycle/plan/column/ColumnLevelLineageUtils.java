/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.package$;

@Slf4j
public class ColumnLevelLineageUtils {

  /**
   * Column level lineage is supported only for Spark 3 and is disabled by default.
   *
   * @return
   */
  public static Optional<OpenLineage.ColumnLineageDatasetFacet> buildColumnLineageDatasetFacet(
      OpenLineageContext context, OpenLineage.SchemaDatasetFacet schemaFacet) {
    // if Spark3 include column lineage
    if (!package$.MODULE$.SPARK_VERSION().startsWith("2")) {
      try {
        return (Optional<OpenLineage.ColumnLineageDatasetFacet>)
            Class.forName(
                    "io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils")
                .getMethod(
                    "buildColumnLineageDatasetFacet",
                    OpenLineageContext.class,
                    OpenLineage.SchemaDatasetFacet.class)
                .invoke(null, context, schemaFacet);
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException e) {
        log.error(
            "Error when invoking static method 'buildColumnLineageDatasetFacet' for Spark3", e);
        return Optional.empty();
      } catch (RuntimeException e) {
        log.error("Error when building column level lineage", e);
        return Optional.empty();
      }
    }
    return Optional.empty();
  }
}
