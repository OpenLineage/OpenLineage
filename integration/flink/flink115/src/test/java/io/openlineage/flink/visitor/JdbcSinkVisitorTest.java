/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcRowOutputFormat;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JdbcSinkVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);

  JdbcSinkVisitor jdbcSinkVisitor = new JdbcSinkVisitor(context);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  @SneakyThrows
  void testIsDefined() {
    assertFalse(jdbcSinkVisitor.isDefinedAt(mock(Object.class)));
    assertTrue(jdbcSinkVisitor.isDefinedAt(mock(JdbcOutputFormat.class)));
    assertTrue(jdbcSinkVisitor.isDefinedAt(mock(JdbcRowOutputFormat.class)));
    assertTrue(jdbcSinkVisitor.isDefinedAt(mock(GenericJdbcSinkFunction.class)));
    assertTrue(jdbcSinkVisitor.isDefinedAt(mock(JdbcXaSinkFunction.class)));
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("provideArguments")
  void testApply(Object source) {
    List<OpenLineage.OutputDataset> outputDatasets = jdbcSinkVisitor.apply(source);

    assertEquals(1, outputDatasets.size());
    assertEquals("postgres://host:5432", outputDatasets.get(0).getNamespace());
    assertEquals("database.jdbc_table", outputDatasets.get(0).getName());
  }

  private static Stream<Arguments> provideArguments() throws Exception {
    JdbcParameterValuesProvider provider = mock(JdbcParameterValuesProvider.class);
    when(provider.getParameterValues()).thenReturn(new String[0][0]);

    JdbcConnectorOptions internalJdbcConnectionOptions =
        new JdbcConnectorOptions.Builder()
            .setDBUrl("jdbc:postgresql://host:5432/database")
            .setTableName("jdbc_table")
            .build();

    JdbcOutputFormat jdbcOutputFormat =
        new JdbcOutputFormat.Builder()
            .setOptions(internalJdbcConnectionOptions)
            .setFieldNames(new String[0])
            .build();

    JdbcOutputFormat jdbcRowOutputFormat =
        new JdbcRowOutputFormat.Builder()
            .setOptions(internalJdbcConnectionOptions)
            .setFieldNames(new String[0])
            .build();

    GenericJdbcSinkFunction genericJdbcSinkFunction = new GenericJdbcSinkFunction(jdbcOutputFormat);
    return Stream.of(Arguments.of(jdbcOutputFormat, jdbcRowOutputFormat, genericJdbcSinkFunction));
  }
}
