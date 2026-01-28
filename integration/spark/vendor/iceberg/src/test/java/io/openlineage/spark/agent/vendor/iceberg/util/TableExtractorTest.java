/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class TableExtractorTest {
  private TableExtractor extractor;
  private Table mockTable;

  @BeforeEach
  void setUp() {
    extractor = new TableExtractor();
    mockTable = mock(Table.class);
  }

  @ParameterizedTest
  @MethodSource("provideTableNamesAndExpectedIdentifiers")
  void extractsTableIdentifier(String tableName, TableIdentifier expected) {
    when(mockTable.name()).thenReturn(tableName);

    Optional<TableIdentifier> result = extractor.extractTableIdentifier(mockTable);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(expected);
  }

  private static Stream<Object[]> provideTableNamesAndExpectedIdentifiers() {
    return Stream.of(
        new Object[] {"catalog.database.table", tableIdentifierOf("database", "table")},
        new Object[] {"database.table", tableIdentifierOf("database", "table")},
        new Object[] {"table", tableIdentifierOf(null, "table")});
  }

  @ParameterizedTest
  @CsvSource(
      value = {"null", "''", "'   '"},
      nullValues = {"null"})
  void returnsEmptyWhenTableNameIsMissing(String tableName) {
    when(mockTable.name()).thenReturn(tableName);

    Optional<TableIdentifier> result = extractor.extractTableIdentifier(mockTable);

    assertThat(result).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("provideTableLocationsAndExpectedResult")
  void extractsTableLocation(String location, String expectedScheme, String expectedPath) {
    when(mockTable.location()).thenReturn(location);

    Optional<URI> result = extractor.extractTableLocation(mockTable);

    assertThat(result).isPresent();
    assertThat(result.get().getScheme()).isEqualTo(expectedScheme);
    assertThat(result.get().getPath()).isEqualTo(expectedPath);
  }

  private static Stream<Object[]> provideTableLocationsAndExpectedResult() {
    return Stream.of(
        new Object[] {"/path/to/table", null, "/path/to/table"},
        new Object[] {"file:///path/to/table", "file", "/path/to/table"},
        new Object[] {"s3://bucket/path/to/table", "s3", "/path/to/table"});
  }

  @ParameterizedTest
  @CsvSource(
      value = {"null", "''", "'   '"},
      nullValues = {"null"})
  void returnsEmptyWhenTableLocationIsMissing(String location) {
    when(mockTable.location()).thenReturn(location);

    Optional<URI> result = extractor.extractTableLocation(mockTable);

    assertThat(result).isEmpty();
  }

  private static TableIdentifier tableIdentifierOf(String database, String table) {
    return new TableIdentifier(table, scala.Option.apply(database));
  }
}
