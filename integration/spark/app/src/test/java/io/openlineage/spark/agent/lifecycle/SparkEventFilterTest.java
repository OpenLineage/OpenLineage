/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.mockito.Mockito.atMost;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ExtendWith(SparkAgentTestExtension.class)
@Tag("integration-test")
@Slf4j
public class SparkEventFilterTest {

  @Test
  @SneakyThrows
  void testCreateViewCommandEventGetsFiltered(SparkSession spark) {
    // CreateViewCommand will not be filtered for Spark 3.2
    if (spark.sparkContext().version().compareTo("3.3") < 0) {
      return;
    }

    Dataset<Row> dataset =
        spark.createDataFrame(
            ImmutableList.of(RowFactory.create(1L, "bat"), RowFactory.create(3L, "horse")),
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("b", StringType$.MODULE$, false, Metadata.empty())
                }));

    dataset.createOrReplaceTempView("temp_view");
    dataset.createGlobalTempView("global_temp_view");
    dataset.count(); // call action
    dataset.collect();

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atMost(0))
        .emit(lineageEvent.capture());
  }
}
