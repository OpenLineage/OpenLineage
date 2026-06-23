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

  @Test
  void qualifiedUnityCatalogTableNameFromTableIdentifier() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply("default"));
    when(identifier.table()).thenReturn("employee_processed_ext");

    assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(identifier))
        .isEqualTo("default.employee_processed_ext");
  }

  @Test
  void qualifiedUnityCatalogTableNameFromTableIdentifierWithCatalog() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply("default"));
    when(identifier.table()).thenReturn("employee_processed_ext");

    try (MockedStatic<MethodUtils> methodUtils = mockStatic(MethodUtils.class)) {
      methodUtils
          .when(() -> MethodUtils.invokeMethod(identifier, "catalog"))
          .thenReturn(Option.apply("main_catalog"));

      assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(identifier))
          .isEqualTo("main_catalog.default.employee_processed_ext");
    }
  }

  @Test
  void qualifiedUnityCatalogTableNameFromV2Identifier() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Identifier identifier = Identifier.of(new String[] {"default"}, "employee_processed_ext");

    when(tableCatalog.name()).thenReturn("main_catalog");

    assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(tableCatalog, identifier))
        .isEqualTo("main_catalog.default.employee_processed_ext");
  }

  @Test
  void qualifiedUnityCatalogTableNameDoesNotDuplicateCatalog() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Identifier identifier =
        Identifier.of(new String[] {"main_catalog", "default"}, "employee_processed_ext");

    when(tableCatalog.name()).thenReturn("main_catalog");

    assertThat(DatabricksUtils.qualifiedUnityCatalogTableName(tableCatalog, identifier))
        .isEqualTo("main_catalog.default.employee_processed_ext");
  }
}
