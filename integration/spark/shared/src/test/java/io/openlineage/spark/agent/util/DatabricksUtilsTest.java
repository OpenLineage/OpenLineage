/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class DatabricksUtilsTest {

  private static final String CATALOG = "main_catalog";
  private static final String SCHEMA = "default";
  private static final String TABLE = "employee_processed_ext";

  @Test
  void qualifiedUnityCatalogTableNameFromTableIdentifier() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply(SCHEMA));
    when(identifier.table()).thenReturn(TABLE);

    assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(identifier))
        .isEqualTo(SCHEMA + "." + TABLE);
  }

  @Test
  void qualifiedUnityCatalogTableNameFromTableIdentifierWithCatalog() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply(SCHEMA));
    when(identifier.table()).thenReturn(TABLE);

    try (MockedStatic<MethodUtils> methodUtils = mockStatic(MethodUtils.class)) {
      methodUtils
          .when(() -> MethodUtils.invokeMethod(identifier, "catalog"))
          .thenReturn(Option.apply(CATALOG));

      assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(identifier))
          .isEqualTo(CATALOG + "." + SCHEMA + "." + TABLE);
    }
  }

  @Test
  void qualifiedUnityCatalogTableNameFromV2Identifier() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Identifier identifier = Identifier.of(new String[] {SCHEMA}, TABLE);

    when(tableCatalog.name()).thenReturn(CATALOG);

    assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(tableCatalog, identifier))
        .isEqualTo(CATALOG + "." + SCHEMA + "." + TABLE);
  }

  @Test
  void qualifiedUnityCatalogTableNameDoesNotDuplicateCatalog() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Identifier identifier = Identifier.of(new String[] {CATALOG, SCHEMA}, TABLE);

    when(tableCatalog.name()).thenReturn(CATALOG);

    assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(tableCatalog, identifier))
        .isEqualTo(CATALOG + "." + SCHEMA + "." + TABLE);
  }
}
