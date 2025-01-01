/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.pojo.Event;
import io.openlineage.flink.utils.CassandraUtils;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CassandraSourceVisitorTest {
  private static String query = "SELECT * FROM flink.source_event;";
  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);
  CassandraSourceVisitor cassandraSourceVisitor = new CassandraSourceVisitor(context);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  @SneakyThrows
  void testIsDefined() {
    assertFalse(cassandraSourceVisitor.isDefinedAt(mock(Object.class)));
    assertTrue(cassandraSourceVisitor.isDefinedAt(mock(CassandraInputFormat.class)));
    assertTrue(cassandraSourceVisitor.isDefinedAt(mock(CassandraPojoInputFormat.class)));
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("provideArguments")
  void testApply(Object source) {
    List<OpenLineage.InputDataset> inputDatasets = cassandraSourceVisitor.apply(source);

    assertEquals(1, inputDatasets.size());
    assertEquals("cassandra://127.0.0.1:9042", inputDatasets.get(0).getNamespace());
    assertEquals("flink.source_event", inputDatasets.get(0).getName());
  }

  private static Stream<Arguments> provideArguments() {
    ClusterBuilder clusterBuilder = CassandraUtils.createClusterBuilder("127.0.0.1");
    CassandraPojoInputFormat pojoOutputFormat =
        new CassandraPojoInputFormat(query, clusterBuilder, Event.class);
    CassandraInputFormat inputFormat = new CassandraInputFormat(query, clusterBuilder);

    return Stream.of(Arguments.of(pojoOutputFormat), Arguments.of(inputFormat));
  }
}
