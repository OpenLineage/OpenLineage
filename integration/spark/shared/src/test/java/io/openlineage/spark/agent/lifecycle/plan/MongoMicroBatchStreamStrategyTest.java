/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Optional;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SparkDataStream;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for MongoMicroBatchStreamStrategy to verify Spark 4.0 constructor support. */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class MongoMicroBatchStreamStrategyTest {

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
    MongoMicroBatchStreamStrategy strategy =
        new MongoMicroBatchStreamStrategy(datasetFactory, schema, stream, offsetOption);

    // Then: Strategy should be created successfully
    assertNotNull(strategy);
  }

  @Test
  void testExtractDatabaseFromURI() {
    // Given: Strategy instance
    StructType schema = new StructType();
    SparkDataStream stream = mock(SparkDataStream.class);
    MongoMicroBatchStreamStrategy strategy =
        new MongoMicroBatchStreamStrategy(datasetFactory, schema, stream, Optional.empty());

    // Note: extractDatabaseFromURI is private, but we can test indirectly through getInputDatasets
    // This test verifies the strategy is constructed correctly for Spark 4.0
    assertNotNull(strategy);
  }

  // ===========================
  // URI Parsing Tests
  // ===========================

  @Test
  void testExtractDatabaseFromURI_NoDatabase() throws Exception {
    // Given: URI without database path
    String uri = "mongodb://host:27017";

    // When: extractDatabaseFromURI is called
    String result = invokeExtractDatabaseFromURI(uri);

    // Then: Should return null
    assertNull(result);
  }

  @Test
  void testExtractDatabaseFromURI_WithDatabase() throws Exception {
    String uri = "mongodb://host:27017/mydb";
    String result = invokeExtractDatabaseFromURI(uri);
    assertEquals("mydb", result);
  }

  @Test
  void testExtractDatabaseFromURI_WithAuthentication() throws Exception {
    String uri = "mongodb://user:pass@host:27017/mydb";
    String result = invokeExtractDatabaseFromURI(uri);
    assertEquals("mydb", result);
  }

  @Test
  void testExtractDatabaseFromURI_WithQueryParams() throws Exception {
    String uri = "mongodb://host:27017/mydb?retryWrites=true&w=majority";
    String result = invokeExtractDatabaseFromURI(uri);
    assertEquals("mydb", result);
  }

  @Test
  void testExtractDatabaseFromURI_MultiHost() throws Exception {
    String uri = "mongodb://host1:27017,host2:27017,host3:27017/mydb";
    String result = invokeExtractDatabaseFromURI(uri);
    assertEquals("mydb", result);
  }

  @Test
  void testExtractDatabaseFromURI_EmptyDatabase() throws Exception {
    String uri = "mongodb://host:27017/";
    String result = invokeExtractDatabaseFromURI(uri);
    assertNull(result);
  }

  @Test
  void testExtractDatabaseFromURI_MalformedURI() throws Exception {
    String uri = "not-a-uri";
    String result = invokeExtractDatabaseFromURI(uri);
    assertNull(result);
  }

  @Test
  void testExtractDatabaseFromURI_EmptyDatabaseWithQueryParams() throws Exception {
    String uri = "mongodb://host:27017/?retryWrites=true";
    String result = invokeExtractDatabaseFromURI(uri);
    assertNull(result);
  }

  // ===========================
  // stripDatabaseFromURI Tests
  // ===========================

  @Test
  void testStripDatabaseFromURI_WithDatabase() throws Exception {
    String uri = "mongodb://host:27017/mydb";
    String result = invokeStripDatabaseFromURI(uri);
    assertEquals("mongodb://host:27017", result);
  }

  @Test
  void testStripDatabaseFromURI_WithDatabaseAndParams() throws Exception {
    String uri = "mongodb://host:27017/mydb?retryWrites=true";
    String result = invokeStripDatabaseFromURI(uri);
    assertEquals("mongodb://host:27017", result);
  }

  @Test
  void testStripDatabaseFromURI_NoDatabase() throws Exception {
    String uri = "mongodb://host:27017";
    String result = invokeStripDatabaseFromURI(uri);
    assertEquals("mongodb://host:27017", result);
  }

  @Test
  void testStripDatabaseFromURI_MultiHost() throws Exception {
    String uri = "mongodb://host1:27017,host2:27017/mydb";
    String result = invokeStripDatabaseFromURI(uri);
    assertEquals("mongodb://host1:27017,host2:27017", result);
  }

  @Test
  void testStripDatabaseFromURI_WithAuthentication() throws Exception {
    String uri = "mongodb://user:pass@host:27017/mydb?retryWrites=true";
    String result = invokeStripDatabaseFromURI(uri);
    assertEquals("mongodb://user:pass@host:27017", result);
  }

  @Test
  void testStripDatabaseFromURI_MalformedURI() throws Exception {
    String uri = "not-a-uri";
    String result = invokeStripDatabaseFromURI(uri);
    // Should return original URI on malformed input
    assertEquals("not-a-uri", result);
  }

  // ===========================
  // Helper Methods
  // ===========================

  /** Helper method to invoke private extractDatabaseFromURI method using reflection. */
  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  private String invokeExtractDatabaseFromURI(String uri) throws Exception {
    StructType schema = new StructType();
    SparkDataStream stream = mock(SparkDataStream.class);
    MongoMicroBatchStreamStrategy strategy =
        new MongoMicroBatchStreamStrategy(datasetFactory, schema, stream, Optional.empty());

    Method method =
        MongoMicroBatchStreamStrategy.class.getDeclaredMethod("extractDatabaseFromURI", String.class);
    method.setAccessible(true);
    return (String) method.invoke(strategy, uri);
  }

  /** Helper method to invoke private stripDatabaseFromURI method using reflection. */
  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  private String invokeStripDatabaseFromURI(String uri) throws Exception {
    StructType schema = new StructType();
    SparkDataStream stream = mock(SparkDataStream.class);
    MongoMicroBatchStreamStrategy strategy =
        new MongoMicroBatchStreamStrategy(datasetFactory, schema, stream, Optional.empty());

    Method method =
        MongoMicroBatchStreamStrategy.class.getDeclaredMethod("stripDatabaseFromURI", String.class);
    method.setAccessible(true);
    return (String) method.invoke(strategy, uri);
  }
}
