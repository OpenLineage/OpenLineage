/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SparkDataStream;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for KinesisMicroBatchStreamStrategy to verify Spark 4.0 constructor support.
 */
class KinesisMicroBatchStreamStrategyTest {

  private OpenLineageContext context;
  private DatasetFactory<OpenLineage.InputDataset> datasetFactory;

  @BeforeEach
  void setUp() {
    context = mock(OpenLineageContext.class);
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    datasetFactory = DatasetFactory.input(context);
  }

  @Test
  void testSpark40Constructor() {
    // Given: Mocked Spark 4.0 streaming components
    StructType schema = new StructType();
    SparkDataStream stream = mock(SparkDataStream.class);
    Optional<Offset> offsetOption = Optional.empty();

    // When: Creating strategy with Spark 4.0 constructor
    KinesisMicroBatchStreamStrategy strategy =
        new KinesisMicroBatchStreamStrategy(datasetFactory, schema, stream, offsetOption);

    // Then: Strategy should be created successfully
    assertNotNull(strategy);
  }
}
