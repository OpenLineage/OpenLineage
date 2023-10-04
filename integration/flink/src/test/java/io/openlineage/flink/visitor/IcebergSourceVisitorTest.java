/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.visitor.wrapper.IcebergSourceWrapper;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.IcebergTableSource;
import org.apache.iceberg.flink.source.StreamingMonitorFunction;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class IcebergSourceVisitorTest {
  static StreamingMonitorFunction monitorFunction = mock(StreamingMonitorFunction.class);
  static IcebergSource icebergSource = mock(IcebergSource.class);
  static IcebergTableSource icebergTableSource = mock(IcebergTableSource.class);

  OpenLineageContext context = mock(OpenLineageContext.class);
  IcebergSourceWrapper wrapper = mock(IcebergSourceWrapper.class);
  IcebergSourceVisitor icebergSourceVisitor = new IcebergSourceVisitor(context);
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  void testIsDefined() {
    assertFalse(icebergSourceVisitor.isDefinedAt(mock(Object.class)));
    assertTrue(icebergSourceVisitor.isDefinedAt(mock(StreamingMonitorFunction.class)));
    assertTrue(icebergSourceVisitor.isDefinedAt(mock(IcebergSource.class)));
    assertTrue(icebergSourceVisitor.isDefinedAt(mock(IcebergTableSource.class)));
  }

  @Test
  void testApplyUnsupported() {
    assertThrows(
        UnsupportedOperationException.class, () -> icebergSourceVisitor.apply(new Object()));
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("provideArguments")
  void testApply(Object sourceObject, Class sourceClass) {
    Table table = mock(Table.class, RETURNS_DEEP_STUBS);

    try (MockedStatic<IcebergSourceWrapper> mockedStatic = mockStatic(IcebergSourceWrapper.class)) {
      when(IcebergSourceWrapper.of(sourceObject, sourceClass)).thenReturn(wrapper);
      when(table.location()).thenReturn("s3://bucket/table/");
      when(table.schema().columns())
          .thenReturn(
              Collections.singletonList(Types.NestedField.of(1, false, "a", Types.LongType.get())));
      when(wrapper.getTable()).thenReturn(table);

      List<OpenLineage.InputDataset> inputDatasets = icebergSourceVisitor.apply(sourceObject);
      List<OpenLineage.SchemaDatasetFacetFields> fields =
          inputDatasets.get(0).getFacets().getSchema().getFields();

      assertEquals(1, inputDatasets.size());
      assertEquals("table", inputDatasets.get(0).getName());
      assertEquals("s3://bucket", inputDatasets.get(0).getNamespace());

      assertEquals(1, fields.size());
      assertEquals("a", fields.get(0).getName());
      assertEquals("LONG", fields.get(0).getType());
    }
  }

  private static Stream<Arguments> provideArguments() {
    return Stream.of(
        Arguments.of(monitorFunction, StreamingMonitorFunction.class),
        Arguments.of(icebergSource, IcebergSource.class),
        Arguments.of(icebergTableSource, IcebergTableSource.class));
  }
}
