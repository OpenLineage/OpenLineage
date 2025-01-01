/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.HashMap;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcHandlerTest {

  OpenLineageContext context = mock(OpenLineageContext.class);

  @BeforeEach
  public void setup() {
    context = mock(OpenLineageContext.class);
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifier() {
    JdbcHandler handler = new JdbcHandler(context);

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
    assertEquals("postgres://postgreshost:5432", datasetIdentifier.getNamespace());
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithDatabase() {
    JdbcHandler handler = new JdbcHandler(context);

    JDBCTableCatalog tableCatalog = new JDBCTableCatalog();
    JDBCOptions options = mock(JDBCOptions.class);
    when(options.url()).thenReturn("jdbc:postgresql://postgreshost:5432/database");
    FieldUtils.writeField(tableCatalog, "options", options, true);

    DatasetIdentifier datasetIdentifier =
        handler.getDatasetIdentifier(
            mock(SparkSession.class),
            tableCatalog,
            Identifier.of(new String[] {"schema"}, "table"),
            new HashMap<>());

    assertEquals("database.schema.table", datasetIdentifier.getName());
    assertEquals("postgres://postgreshost:5432", datasetIdentifier.getNamespace());
  }
}
