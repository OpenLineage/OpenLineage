/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BaseCatalogTypeHandlerTest {
  private static final Identifier IDENTIFIER = Identifier.of(new String[] {"db"}, "events");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "events");
  private static SparkSession session;

  @TempDir Path warehouse;

  private final BaseCatalogTypeHandler handler = new HadoopCatalogTypeHandler();

  @BeforeAll
  static void startSpark(@TempDir Path sparkDirectory) {
    session =
        SparkSession.builder()
            .master("local[1]")
            .appName("iceberg-catalog-lookup-test")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.local.dir", sparkDirectory.toString())
            .config("spark.sql.warehouse.dir", sparkDirectory.resolve("warehouse").toString())
            .getOrCreate();
    session.sparkContext().setLogLevel("WARN");
  }

  @AfterAll
  static void stopSpark() {
    session.stop();
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lookupDoesNotReplaceAnExpiredWriter(boolean sessionCatalog) throws Exception {
    SparkCatalog sparkCatalog = catalog(true);
    Catalog cache = sparkCatalog.icebergCatalog();
    Table writer = createTable(cache);
    append(writer, "first.parquet");
    long firstSnapshot = writer.currentSnapshot().snapshotId();

    Thread.sleep(1100);
    assertThat(handler.getIcebergTable(asCatalog(sparkCatalog, sessionCatalog), IDENTIFIER))
        .get()
        .extracting(table -> table.currentSnapshot().snapshotId())
        .isEqualTo(firstSnapshot);

    append(writer, "second.parquet");
    assertThat(cache.loadTable(TABLE_IDENTIFIER).currentSnapshot().snapshotId())
        .isEqualTo(writer.currentSnapshot().snapshotId())
        .isNotEqualTo(firstSnapshot);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lookupDoesNotPopulateTheSessionCache(boolean sessionCatalog) {
    SparkCatalog sparkCatalog = catalog(true);
    Catalog cache = sparkCatalog.icebergCatalog();
    createTable(cache);
    cache.invalidateTable(TABLE_IDENTIFIER);

    Table observed =
        handler.getIcebergTable(asCatalog(sparkCatalog, sessionCatalog), IDENTIFIER).get();

    assertThat(cache.loadTable(TABLE_IDENTIFIER)).isNotSameAs(observed);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lookupPreservesCaseInsensitiveIdentifiers(boolean sessionCatalog) {
    SparkCatalog sparkCatalog = catalog(false);
    Table table = createTable(sparkCatalog.icebergCatalog());
    Identifier uppercase = Identifier.of(new String[] {"DB"}, "EVENTS");

    assertThat(handler.getIcebergTable(asCatalog(sparkCatalog, sessionCatalog), uppercase))
        .get()
        .extracting(Table::location)
        .isEqualTo(table.location());
  }

  @Test
  void lookupPreservesMetadataTablesAndSnapshotSelectors() {
    SparkCatalog sparkCatalog = catalog(true);
    Table table = createTable(sparkCatalog.icebergCatalog());
    append(table, "first.parquet");
    long snapshot = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("audit").createTag("release", snapshot).commit();

    for (String selector :
        new String[] {
          "snapshots",
          "snapshot_id_" + snapshot,
          "at_timestamp_" + table.currentSnapshot().timestampMillis(),
          "branch_audit",
          "tag_release"
        }) {
      Identifier identifier = Identifier.of(new String[] {"db", "events"}, selector);
      Optional<Table> expected;
      try {
        expected = IcebergSparkTables.fromIcebergSparkTable(sparkCatalog.loadTable(identifier));
      } catch (NoSuchTableException e) {
        expected = Optional.empty();
      }
      Optional<Table> observed = handler.getIcebergTable(sparkCatalog, identifier);
      assertThat(observed.map(Table::location))
          .as(selector)
          .isEqualTo(expected.map(Table::location));
      assertThat(observed.map(Object::getClass))
          .as(selector)
          .isEqualTo(expected.map(Object::getClass));
    }
  }

  @Test
  void lookupPreservesPathIdentifiers() {
    SparkCatalog sparkCatalog = catalog(true);
    Table table = createTable(sparkCatalog.icebergCatalog());

    assertThat(handler.getIcebergTable(sparkCatalog, new PathIdentifier(table.location())))
        .get()
        .extracting(Table::location)
        .isEqualTo(table.location());
  }

  @Test
  void lookupRestoresTheListenerThreadsActiveSession() throws Exception {
    SparkCatalog catalog = catalog(true);
    Table table = createTable(catalog.icebergCatalog());

    Optional<Table> observed =
        CompletableFuture.supplyAsync(
                () -> {
                  SparkSession.clearActiveSession();
                  Optional<Table> result = handler.getIcebergTable(catalog, IDENTIFIER);
                  assertThat(SparkSession.getActiveSession().isDefined()).isFalse();
                  return result;
                })
            .get(10, TimeUnit.SECONDS);

    assertThat(observed).get().extracting(Table::location).isEqualTo(table.location());
  }

  private SparkCatalog catalog(boolean caseSensitive) {
    SparkCatalog catalog = new SparkCatalog();
    Map<String, String> options = new HashMap<>();
    options.put("type", "hadoop");
    options.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString());
    options.put(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS, "1000");
    options.put(CatalogProperties.CACHE_CASE_SENSITIVE, Boolean.toString(caseSensitive));
    catalog.initialize("test", new CaseInsensitiveStringMap(options));
    return catalog;
  }

  private TableCatalog asCatalog(SparkCatalog catalog, boolean sessionCatalog) {
    if (!sessionCatalog) {
      return catalog;
    }
    SparkSessionCatalog<?> wrapper = mock(SparkSessionCatalog.class);
    when(wrapper.icebergCatalog()).thenReturn(catalog.icebergCatalog());
    return wrapper;
  }

  private Table createTable(Catalog catalog) {
    return catalog.createTable(
        TABLE_IDENTIFIER, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
  }

  private void append(Table table, String filename) {
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(warehouse.resolve(filename).toString())
                .withFileSizeInBytes(1)
                .withRecordCount(1)
                .build())
        .commit();
  }
}
