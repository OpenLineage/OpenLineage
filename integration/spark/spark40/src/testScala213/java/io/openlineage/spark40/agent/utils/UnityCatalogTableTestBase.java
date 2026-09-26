/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.TestIds;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Base for tests that need a single Delta table registered in the embedded Unity Catalog and an
 * {@link OpenLineageContext} to resolve it.
 *
 * <p>{@link #setUp()} runs the common skeleton — create the session and catalog, allocate a table
 * location, create the schema, provision the table, resolve the catalog handle, build the context —
 * and delegates the two parts that vary to {@link #extraSparkConfig()} (extra session config, e.g.
 * cloud credentials) and {@link #provisionTable()} (how the table at {@link #tableLocation} is
 * created). Subclasses add only their {@code @Test} methods and these two hooks.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class UnityCatalogTableTestBase extends SparkUnityCatalogTestBase {

  protected final String catalogName = TestIds.randomName("test_catalog_");
  protected final String schemaName = TestIds.randomName("test_schema_");
  protected final String tableName = TestIds.randomName("test_table_");

  protected String tableLocation;
  protected OpenLineageContext context;
  protected TableCatalog unityCatalog;

  @BeforeAll
  void setUp() throws Exception {
    createWithCatalog(catalogName, extraSparkConfig());
    spark.sql(String.format("CREATE SCHEMA %s.%s", catalogName, schemaName));
    tableLocation = externalTablesManager.getLocation(catalogName, schemaName, tableName);
    provisionTable();
    unityCatalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    context =
        OpenLineageContext.builder()
            .sparkSession(spark)
            .sparkContext(spark.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();
  }

  /** Extra Spark configuration for the session; empty by default. */
  protected Map<String, String> extraSparkConfig() {
    return Collections.emptyMap();
  }

  /**
   * Creates the test table at {@link #tableLocation}. The session, catalog and schema already
   * exist; the resolved catalog handle and {@link #context} are built afterward.
   */
  protected abstract void provisionTable() throws Exception;

  @AfterAll
  void cleanUp() {
    cleanUpSession();
  }
}
