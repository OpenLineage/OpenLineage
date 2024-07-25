/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory.FAILING_TABLE_NAME_FAIL_ON_APPLY;
import static io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory.FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import scala.collection.immutable.HashMap;

/**
 * Class containing tests to verify that Spark job executes properly even when problems related to
 * OpenLineage client encountered
 */
@Slf4j
@ExtendWith({EventEmitterProviderExtension.class, SparkAgentTestExtension.class})
class SparkOpenLineageClientFailuresTest {
  @Test
  void testOpenLineageFailingVisitor(EventEmitter eventEmitter, SparkSession spark)
      throws InterruptedException, TimeoutException {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("aString", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    Dataset<Row> dataset =
        spark.createDataFrame(Arrays.asList(new GenericRow(new Object[] {"one"})), schema);

    dataset.write().mode(SaveMode.Overwrite).saveAsTable(FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED);
    dataset.write().mode(SaveMode.Overwrite).saveAsTable(FAILING_TABLE_NAME_FAIL_ON_APPLY);
    // creating table that way should fail because of a failing visitor defined in
    // TestOpenLineageEventHandlerFactory

    StaticExecutionContextFactory.waitForExecutionEnd();
    assertEquals(1, spark.table(FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED).count());
    assertEquals(1, spark.table(FAILING_TABLE_NAME_FAIL_ON_APPLY).count());

    ArgumentMatcher<OpenLineage.RunEvent> matcher =
        new ArgumentMatcher<OpenLineage.RunEvent>() {
          @Override
          public boolean matches(OpenLineage.RunEvent event) {
            return event.getOutputs().size() == 1
                && event.getEventType().equals(OpenLineage.RunEvent.EventType.COMPLETE)
                && event.getOutputs().get(0).getName().contains("failing_table");
          }
        };

    Mockito.verify(eventEmitter, Mockito.atLeast(2)).emit(argThat(matcher));
  }
}
