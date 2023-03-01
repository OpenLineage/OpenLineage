/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
public class LogicalRelationBuilderTest {

  StructType structTypeSchema =
      new StructType(
          new StructField[] {
            new StructField("a", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("b", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
          });

  @Test
  void logicalRelationOnlyPlanHasNoOutput(SparkSession spark)
      throws InterruptedException, TimeoutException {
    spark
        .createDataFrame(Arrays.asList(new GenericRow(new Object[] {1, 2})), structTypeSchema)
        .write()
        .saveAsTable("temp");
    spark.sql("select * from temp").collect();

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(1))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();

    assertThat(events.get(events.size() - 1).getOutputs()).isEmpty();
  }

  @Test
  void logicalRelationPlanHasInputAndOutput(SparkSession spark)
      throws InterruptedException, TimeoutException {
    spark
        .createDataFrame(Arrays.asList(new GenericRow(new Object[] {1, 2})), structTypeSchema)
        .write()
        .saveAsTable("temp");
    StaticExecutionContextFactory.waitForExecutionEnd();

    spark.sql("select * from temp").write().saveAsTable("output_table");

    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(1))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();

    assertThat(events.get(events.size() - 1).getInputs().get(0).getName()).endsWith("temp");
    assertThat(events.get(events.size() - 1).getOutputs()).hasSize(1);

    // verify only output table is contained within outputs
    assertThat(events.get(events.size() - 1).getOutputs().get(0).getName())
        .endsWith("output_table");
  }
}
