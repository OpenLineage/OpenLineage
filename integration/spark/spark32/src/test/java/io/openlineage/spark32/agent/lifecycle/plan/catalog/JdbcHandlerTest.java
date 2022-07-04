/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.HashMap;
import lombok.SneakyThrows;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.junit.jupiter.api.Test;

class JdbcHandlerTest {

  @Test
  @SneakyThrows
  void testGetDatasetIdentifier() {
    JdbcHandler handler = new JdbcHandler();

    JDBCTableCatalog tableCatalog = new JDBCTableCatalog();
    JDBCOptions options = mock(JDBCOptions.class);
    when(options.url()).thenReturn("jdbc:postgresql://postgreshost:5432");
    FieldUtils.writeField(tableCatalog, "options", options, true);

    DatasetIdentifier datasetIdentifier =
        handler.getDatasetIdentifier(
            mock(SparkSession.class),
            tableCatalog,
            Identifier.of(new String[] {"database", "schema"}, "table"),
            new HashMap<>());

    assertEquals("database.schema.table", datasetIdentifier.getName());
    assertEquals("postgresql://postgreshost:5432", datasetIdentifier.getNamespace());
  }
}
