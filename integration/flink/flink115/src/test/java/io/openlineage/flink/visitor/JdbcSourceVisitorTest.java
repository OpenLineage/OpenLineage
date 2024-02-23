/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import lombok.SneakyThrows;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JdbcSourceVisitorTest {
  static String QUERY = "select * from jdbc_table";
  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);

  JdbcSourceVisitor jdbcSourceVisitor = new JdbcSourceVisitor(context);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  @SneakyThrows
  public void testIsDefined() {
    assertFalse(jdbcSourceVisitor.isDefinedAt(mock(Object.class)));
    assertTrue(jdbcSourceVisitor.isDefinedAt(mock(JdbcInputFormat.class)));
    assertTrue(jdbcSourceVisitor.isDefinedAt(mock(JdbcRowDataInputFormat.class)));
    assertTrue(jdbcSourceVisitor.isDefinedAt(mock(JdbcRowDataLookupFunction.class)));
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testApply(Object source) {
    List<OpenLineage.InputDataset> inputDatasets = jdbcSourceVisitor.apply(source);

    assertEquals(1, inputDatasets.size());
    assertEquals("jdbc:postgresql://host:port/database", inputDatasets.get(0).getNamespace());
    assertEquals("jdbc_table", inputDatasets.get(0).getName());
  }

  private static Stream<Arguments> provideArguments() {
    JdbcParameterValuesProvider provider = mock(JdbcParameterValuesProvider.class);
    when(provider.getParameterValues()).thenReturn(new String[0][0]);
    JdbcInputFormat jdbcInputFormat =
        new JdbcInputFormat.JdbcInputFormatBuilder()
            .setDBUrl("jdbc:postgresql://host:port/database")
            .setRowTypeInfo(mock(RowTypeInfo.class))
            .setParametersProvider(provider)
            .setQuery(QUERY)
            .finish();

    JdbcRowDataInputFormat jdbcRowDataInputFormat =
        new JdbcRowDataInputFormat.Builder()
            .setDBUrl("jdbc:postgresql://host:port/database")
            .setParametersProvider(provider)
            .setRowConverter(mock(JdbcRowConverter.class))
            .setQuery(QUERY)
            .build();

    JdbcConnectorOptions internalJdbcConnectionOptions =
        new JdbcConnectorOptions.Builder()
            .setDBUrl("jdbc:postgresql://host:port/database")
            .setTableName("jdbc_table")
            .build();
    JdbcRowDataLookupFunction jdbcRowDataLookupFunction =
        new JdbcRowDataLookupFunction(
            internalJdbcConnectionOptions,
            JdbcLookupOptions.builder().setCacheMaxSize(1000).build(),
            new String[0],
            new DataType[0],
            new String[0],
            mock(RowType.class));

    return Stream.of(
        Arguments.of(jdbcInputFormat, jdbcRowDataInputFormat, jdbcRowDataLookupFunction));
  }
}
