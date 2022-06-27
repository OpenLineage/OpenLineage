/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
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
@Tag("spark3")
class DeltaDataSourceTest {

  @Test
  void testInsertIntoDeltaSource(@TempDir Path tempDir, SparkSession spark)
      throws IOException, InterruptedException, TimeoutException {
    StructType tableSchema =
        new StructType(
            new StructField[] {
              new StructField("name", StringType$.MODULE$, false, Metadata.empty()),
              new StructField("age", LongType$.MODULE$, false, Metadata.empty())
            });
    Dataset<Row> df =
        spark.createDataFrame(
            Arrays.asList(
                new GenericRowWithSchema(new Object[] {"john", 25L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"sam", 22L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"alicia", 35L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"bob", 47L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"jordan", 52L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"liz", 19L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"marcia", 83L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"maria", 40L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"luis", 8L}, tableSchema),
                new GenericRowWithSchema(new Object[] {"gabriel", 30L}, tableSchema)),
            tableSchema);

    String deltaDir = tempDir.resolve("deltaData").toAbsolutePath().toString();
    df.write().format("delta").option("path", deltaDir).mode(SaveMode.Overwrite).save();
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(RunEvent.class);

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, Mockito.atLeast(2))
        .emit(lineageEvent.capture());
    List<RunEvent> events = lineageEvent.getAllValues();
    Optional<RunEvent> completionEvent =
        events.stream()
            .filter(e -> e.getEventType().equals(EventType.COMPLETE) && !e.getOutputs().isEmpty())
            .findFirst();
    assertTrue(completionEvent.isPresent());
    RunEvent event = completionEvent.get();
    List<OpenLineage.OutputDataset> outputs = event.getOutputs();
    assertEquals(1, outputs.size());
    assertEquals("file", outputs.get(0).getNamespace());
    assertEquals(deltaDir, outputs.get(0).getName());
  }
}
