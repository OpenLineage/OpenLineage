package io.openlineage.spark.agent.lifecycle.plan.columnLineage;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.package$;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class ColumnLevelLineageUtils {

  /**
   * Column level lineage is supported only for Spark 3 and is disabled by default.
   *
   * @return
   */
  public static Optional<OpenLineage.ColumnLineageDatasetFacet> buildColumnLineageDatasetFacet(
      OpenLineageContext context, StructType schema) {
    // if Spark3 include column lineage
    if (!package$.MODULE$.SPARK_VERSION().startsWith("2")) {
      try {
        return (Optional<OpenLineage.ColumnLineageDatasetFacet>)
            Class.forName(
                    "io.openlineage.spark3.agent.lifecycle.plan.columnLineage.ColumnLevelLineageUtils")
                .getMethod(
                    "buildColumnLineageDatasetFacet", OpenLineageContext.class, StructType.class)
                .invoke(context, schema);
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | IllegalAccessException
          | InvocationTargetException e) {
        log.error("Error when invoking static method 'buildColumnLineageDatasetFacet' for Spark3");
        return Optional.empty();
      } catch (RuntimeException e) {
        log.error("Error when building column level lineage", e);
        return Optional.empty();
      }
    }
    return Optional.empty();
  }
}
