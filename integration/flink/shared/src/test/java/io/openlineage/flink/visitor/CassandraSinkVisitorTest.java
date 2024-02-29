/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraRowOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.CassandraPojoSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraRowSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraRowWriteAheadSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraTupleSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraTupleWriteAheadSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CassandraSinkVisitorTest {
  private static String insertQuery = "INSERT INTO flink.sink_event (id) VALUES (uuid());";
  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);
  CassandraSinkVisitor cassandraSinkVisitor = new CassandraSinkVisitor(context);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  @SneakyThrows
  public void testIsDefined() {
    assertFalse(cassandraSinkVisitor.isDefinedAt(mock(Object.class)));
    // Use Pojo class
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraPojoOutputFormat.class)));
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraPojoSink.class)));
    // Use insert query
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraRowOutputFormat.class)));
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraTupleOutputFormat.class)));
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraTupleWriteAheadSink.class)));
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraTupleSink.class)));
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraRowSink.class)));
    assertTrue(cassandraSinkVisitor.isDefinedAt(mock(CassandraRowWriteAheadSink.class)));
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testApply(Object sink) {
    List<OpenLineage.OutputDataset> outputDatasets = cassandraSinkVisitor.apply(sink);

    assertEquals(1, outputDatasets.size());
    assertEquals("cassandra://127.0.0.1:9042", outputDatasets.get(0).getNamespace());
    assertEquals("flink.sink_event", outputDatasets.get(0).getName());
  }

  private static Stream<Arguments> provideArguments() {
    ClusterBuilder clusterBuilder = CassandraUtils.createClusterBuilder("127.0.0.1");
    CassandraPojoOutputFormat pojoOutputFormat =
        new CassandraPojoOutputFormat(clusterBuilder, Event.class);
    CassandraPojoSink pojoSink = new CassandraPojoSink(Event.class, clusterBuilder);
    CassandraRowOutputFormat rowOutputFormat =
        new CassandraRowOutputFormat(insertQuery, clusterBuilder);
    CassandraTupleOutputFormat tupleOutputFormat =
        new CassandraTupleOutputFormat(insertQuery, clusterBuilder);
    CassandraTupleSink tupleWriteAheadSink = new CassandraTupleSink(insertQuery, clusterBuilder);
    CassandraRowSink rowSink = new CassandraRowSink(1, insertQuery, clusterBuilder);

    return Stream.of(
        Arguments.of(pojoOutputFormat),
        Arguments.of(pojoSink),
        Arguments.of(rowOutputFormat),
        Arguments.of(tupleOutputFormat),
        Arguments.of(tupleWriteAheadSink),
        Arguments.of(rowSink));
  }
}
