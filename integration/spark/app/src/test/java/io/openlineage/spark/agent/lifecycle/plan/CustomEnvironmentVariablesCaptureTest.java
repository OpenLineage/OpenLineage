/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ExtendWith(SparkAgentTestExtension.class)
@Tag("custom_environment_variables_capture")
class CustomEnvironmentVariablesCaptureTest {
  protected static void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.put(key, value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }

  @Test
  void testCustomVariableCapture(@TempDir Path tempDir, SparkSession spark)
      throws InterruptedException, TimeoutException {
    CustomEnvironmentVariablesCaptureTest.setEnv("TEST_VAR", "test");
    StructType tableSchema =
        new StructType(
            new StructField[] {
              new StructField("name", StringType$.MODULE$, false, Metadata.empty()),
              new StructField("age", LongType$.MODULE$, false, Metadata.empty())
            });
    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(new GenericRowWithSchema(new Object[] {"john", 25L}, tableSchema)),
            tableSchema);

    String deltaDir = tempDir.resolve("parquetData").toAbsolutePath().toString();
    df.write().format("parquet").option("path", deltaDir).mode(SaveMode.Overwrite).save();
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(RunEvent.class);

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, Mockito.atLeast(2))
        .emit(lineageEvent.capture());
    List<RunEvent> events = lineageEvent.getAllValues();
    Optional<RunEvent> startEvent =
        events.stream()
            .filter(
                e ->
                    e.getEventType().equals(EventType.START)
                        && e.getRun()
                                .getFacets()
                                .getAdditionalProperties()
                                .get("environment-properties")
                            != null)
            .findFirst();
    assertTrue(startEvent.isPresent());
    RunEvent event = startEvent.get();
    EnvironmentFacet additionalProperties =
        (EnvironmentFacet)
            event.getRun().getFacets().getAdditionalProperties().get("environment-properties");
    assertEquals(additionalProperties.getProperties().get("TEST_VAR"), System.getenv("TEST_VAR"));
  }
}
