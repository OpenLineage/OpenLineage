/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder.VENDOR_CONTEXT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.VendorsContext;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.BinaryCommand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryCommand;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;

public class IcebergMetricsReporterInjectorTest {

  private static final String HADOOP_CATALOG_NAME = "hadoop";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "t");

  @TempDir Path warehouse;

  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  VendorsContext vendorsContext = new VendorsContext();
  IcebergMetricsReporterInjector injector;
  LogicalPlan plan;
  LogicalPlan subPlan;
  CachingCatalog cachingCatalog;
  TestingIcebergCatalog icebergCatalog;
  MetricsReporter existingMetricsReporter;

  @BeforeEach
  void setup() {
    injector = new IcebergMetricsReporterInjector(context);
    icebergCatalog = new TestingIcebergCatalog();

    plan = mock(LogicalPlan.class, withSettings().extraInterfaces(UnaryCommand.class));
    subPlan =
        mock(
            LogicalPlan.class, withSettings().extraInterfaces(TestingLogicalPlanWithCatalog.class));

    when(((UnaryCommand) plan).child()).thenReturn(subPlan);

    when(context.getOpenLineageConfig().getVendors().getConfig())
        .thenReturn(
            Collections.singletonMap(
                "iceberg", new SparkOpenLineageConfig.VendorsConfig.VendorConfig(false)));
    when(context.getVendors().getVendorsContext()).thenReturn(vendorsContext);

    cachingCatalog = (CachingCatalog) CachingCatalog.wrap(icebergCatalog);
  }

  @ParameterizedTest
  @MethodSource("provideCatalogs")
  void testIsDefinedForIcebergCatalog(CatalogPlugin catalog) {
    setupCatalog(catalog);

    assertThat(injector.isDefinedAt(mock(LogicalPlan.class))).isFalse();
    assertThat(injector.isDefinedAt(plan)).isTrue();

    LogicalPlan planWithCatalogMethod =
        mock(
            LogicalPlan.class, withSettings().extraInterfaces(TestingLogicalPlanWithCatalog.class));
    when(((TestingLogicalPlanWithCatalog) planWithCatalogMethod).catalog()).thenReturn(catalog);
    assertThat(injector.isDefinedAt(planWithCatalogMethod)).isTrue();

    LogicalPlan binaryCommand =
        mock(LogicalPlan.class, withSettings().extraInterfaces(BinaryCommand.class));
    when(((BinaryCommand) binaryCommand).left()).thenReturn(subPlan);
    assertThat(injector.isDefinedAt(binaryCommand)).isTrue();

    DataSourceV2ScanRelation v2ScanRelation =
        mock(DataSourceV2ScanRelation.class, RETURNS_DEEP_STUBS);
    when(v2ScanRelation.relation().catalog().get()).thenReturn(catalog);
    assertThat(injector.isDefinedAt(v2ScanRelation)).isTrue();

    DataSourceV2Relation v2Relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(v2Relation.catalog().get()).thenReturn(catalog);
    assertThat(injector.isDefinedAt(v2Relation)).isTrue();
  }

  @Test
  @SneakyThrows
  void testCachedTableCatalogIsSkipped() {
    String catalogClass = "org.apache.iceberg.spark.SparkCachedTableCatalog";
    // Older Iceberg dependencies may not provide the cached catalog.
    assumeTrue(ReflectionUtils.hasClass(catalogClass));
    CatalogPlugin catalog =
        (CatalogPlugin) Class.forName(catalogClass).getDeclaredConstructor().newInstance();
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(relation.catalog()).thenReturn(Option.apply(catalog));

    assertThat(injector.apply(relation)).isEmpty();
    assertThat(injector.isDefinedAt(relation)).isFalse();
    assertThat(vendorsContext.fromVendorsContext(VENDOR_CONTEXT_KEY)).isEmpty();
  }

  @Test
  void testAbsentCatalogIsSkipped() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(relation.catalog()).thenReturn(Option.empty());

    assertThat(injector.isDefinedAt(relation)).isFalse();
    assertThat(injector.apply(relation)).isEmpty();
    assertThat(injector.apply(mock(LogicalPlan.class))).isEmpty();
    assertThat(vendorsContext.fromVendorsContext(VENDOR_CONTEXT_KEY)).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("provideCatalogs")
  void testNullUnderlyingCatalogIsSkipped(CatalogPlugin catalog) {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(relation.catalog()).thenReturn(Option.apply(catalog));

    assertThat(injector.isDefinedAt(relation)).isTrue();
    verify((HasIcebergCatalog) catalog, never()).icebergCatalog();
    assertThat(injector.apply(relation)).isEmpty();
    assertThat(vendorsContext.fromVendorsContext(VENDOR_CONTEXT_KEY)).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("provideCatalogs")
  void testIsDefinedWhenMetricsReporterDisabled(CatalogPlugin catalog) {
    setupCatalog(catalog);

    when(context.getOpenLineageConfig().getVendors().getConfig())
        .thenReturn(
            Collections.singletonMap(
                "iceberg", new SparkOpenLineageConfig.VendorsConfig.VendorConfig(true)));
    assertThat(injector.isDefinedAt(plan)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("provideCatalogs")
  @SneakyThrows
  void testApplyInjectsMetricsReporter(CatalogPlugin catalog) {
    setupCatalog(catalog);

    FieldUtils.writeField(icebergCatalog, "metricsReporter", null, true);
    injector.apply(plan);

    CatalogMetricsReporterHolder holder =
        (CatalogMetricsReporterHolder)
            context.getVendors().getVendorsContext().fromVendorsContext(VENDOR_CONTEXT_KEY).get();

    assertThat(getMetricsReporter(icebergCatalog)).isEqualTo(holder.getReporterFor("catalog-name"));
  }

  @ParameterizedTest
  @MethodSource("provideCatalogs")
  @SneakyThrows
  void testApplyInjectsMetricReporterWithExistingReporter(CatalogPlugin catalog) {
    setupCatalog(catalog);

    FieldUtils.writeField(icebergCatalog, "metricsReporter", existingMetricsReporter, true);
    injector.apply(plan);
    CatalogMetricsReporterHolder holder =
        (CatalogMetricsReporterHolder)
            context.getVendors().getVendorsContext().fromVendorsContext(VENDOR_CONTEXT_KEY).get();

    assertThat(holder.getReporterFor("catalog-name").getDelegate())
        .isEqualTo(existingMetricsReporter);

    assertThat(getMetricsReporter(icebergCatalog)).isEqualTo(holder.getReporterFor("catalog-name"));
  }

  @Test
  @SneakyThrows
  void testApplyAttachesReporterToWriteTargetLoadedBeforeInjection() {
    MetricsReporter catalogReporter = mock(MetricsReporter.class);
    Catalog catalog = cachingHadoopCatalog(catalogReporter);
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    // Spark loads the target table during analysis, before OpenLineage sees the plan
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    // the write target is not a child of AppendData and the query does not read Iceberg
    LogicalPlan append = appendData(relation(catalog, table));
    assertThat(injector.isDefinedAt(append)).isTrue();
    injector.apply(append);

    long snapshotId = commitAppend(table);

    assertThat(holder().getReporterFor(HADOOP_CATALOG_NAME).getCommitReportFacets()).hasSize(1);
    assertThat(holder().getCommitReportFacet(snapshotId)).isPresent();
    // the reporter the table was created with still receives the report exactly once
    verify(catalogReporter, times(1)).report(any(CommitReport.class));
  }

  @Test
  @SneakyThrows
  void testApplyAttachesReporterToReadRelationLoadedBeforeInjection() {
    Catalog catalog = cachingHadoopCatalog(mock(MetricsReporter.class));
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    // e.g. MERGE: the injector reaches the target table through the read side of the plan
    injector.apply(relation(catalog, table));

    long snapshotId = commitAppend(table);
    assertThat(holder().getCommitReportFacet(snapshotId)).isPresent();
  }

  @Test
  @SneakyThrows
  void testApplyAttachesReporterToStreamingWriteTarget() {
    Catalog catalog = cachingHadoopCatalog(mock(MetricsReporter.class));
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    WriteToDataSourceV2 write = mock(WriteToDataSourceV2.class);
    DataSourceV2Relation relation = relation(catalog, table);
    when(write.relation()).thenReturn(Option.apply(relation));
    when(write.query()).thenReturn(mock(LogicalPlan.class));

    assertThat(injector.isDefinedAt(write)).isTrue();
    injector.apply(write);

    long snapshotId = commitAppend(table);
    assertThat(holder().getCommitReportFacet(snapshotId)).isPresent();
  }

  @Test
  @SneakyThrows
  void testApplyAttachesReporterToWrappedTable() {
    Catalog catalog = cachingHadoopCatalog(mock(MetricsReporter.class));
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    // row-level operations wrap the Iceberg SparkTable in a RowLevelOperationTable
    org.apache.spark.sql.connector.catalog.Table wrapper =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            withSettings().extraInterfaces(TestingTableWrapper.class));
    when(((TestingTableWrapper) wrapper).table()).thenReturn(new SparkTable(table, false));
    DataSourceV2Relation relation = relation(catalog, table);
    when(relation.table()).thenReturn(wrapper);

    injector.apply(appendData(relation));

    long snapshotId = commitAppend(table);
    assertThat(holder().getCommitReportFacet(snapshotId)).isPresent();
  }

  @Test
  @SneakyThrows
  void testApplyAttachesReporterOnlyOnce() {
    MetricsReporter catalogReporter = mock(MetricsReporter.class);
    Catalog catalog = cachingHadoopCatalog(catalogReporter);
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    DataSourceV2Relation relation = relation(catalog, table);

    injector.apply(appendData(relation));
    MetricsReporter attached = tableReporter(table);
    injector.apply(relation);
    injector.apply(appendData(relation));

    assertThat(tableReporter(table)).isSameAs(attached);

    long snapshotId = commitAppend(table);
    assertThat(holder().getReporterFor(HADOOP_CATALOG_NAME).getCommitReportFacets()).hasSize(1);
    assertThat(holder().getCommitReportFacet(snapshotId)).isPresent();
    verify(catalogReporter, times(1)).report(any(CommitReport.class));
  }

  @Test
  @SneakyThrows
  void testApplyKeepsReporterOfTableCreatedAfterInjection() {
    MetricsReporter catalogReporter = mock(MetricsReporter.class);
    Catalog catalog = cachingHadoopCatalog(catalogReporter);
    TableIdentifier first = TableIdentifier.of("db", "first");
    catalog.createTable(first, SCHEMA);
    injector.apply(relation(catalog, catalog.loadTable(first)));

    // a table created after injection already reports to the OpenLineage reporter
    Table table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    MetricsReporter reporter = tableReporter(table);
    injector.apply(appendData(relation(catalog, table)));
    assertThat(tableReporter(table)).isSameAs(reporter);

    long snapshotId = commitAppend(table);
    assertThat(holder().getReporterFor(HADOOP_CATALOG_NAME).getCommitReportFacets()).hasSize(1);
    assertThat(holder().getCommitReportFacet(snapshotId)).isPresent();
    verify(catalogReporter, times(1)).report(any(CommitReport.class));
  }

  @Test
  @SneakyThrows
  void testWriteTargetIsSkippedWhenMetricsReporterDisabled() {
    when(context.getOpenLineageConfig().getVendors().getConfig())
        .thenReturn(
            Collections.singletonMap(
                "iceberg", new SparkOpenLineageConfig.VendorsConfig.VendorConfig(true)));
    Catalog catalog = cachingHadoopCatalog(mock(MetricsReporter.class));
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    assertThat(injector.isDefinedAt(appendData(relation(catalog, table)))).isFalse();
  }

  private Catalog cachingHadoopCatalog(MetricsReporter reporter) throws IllegalAccessException {
    HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehouse.toString());
    FieldUtils.writeField(hadoopCatalog, "metricsReporter", reporter, true);
    return CachingCatalog.wrap(hadoopCatalog);
  }

  private DataSourceV2Relation relation(Catalog icebergCatalog, Table table) {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.icebergCatalog()).thenReturn(icebergCatalog);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(relation.catalog()).thenReturn(Option.apply(sparkCatalog));
    when(relation.table()).thenReturn(new SparkTable(table, false));
    return relation;
  }

  private LogicalPlan appendData(DataSourceV2Relation relation) {
    AppendData append = mock(AppendData.class);
    LogicalPlan query = mock(LogicalPlan.class);
    when(append.table()).thenReturn(relation);
    when(append.query()).thenReturn(query);
    when(append.child()).thenReturn(query);
    return append;
  }

  private long commitAppend(Table table) {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(warehouse.resolve(UUID.randomUUID() + ".parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withFormat(FileFormat.PARQUET)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    return table.currentSnapshot().snapshotId();
  }

  @SneakyThrows
  private MetricsReporter tableReporter(Table table) {
    return (MetricsReporter) FieldUtils.readField(table, "reporter", true);
  }

  private CatalogMetricsReporterHolder holder() {
    return (CatalogMetricsReporterHolder)
        vendorsContext.fromVendorsContext(VENDOR_CONTEXT_KEY).get();
  }

  public interface TestingTableWrapper {
    org.apache.spark.sql.connector.catalog.Table table();
  }

  private void setupCatalog(CatalogPlugin catalog) {
    when(((TestingLogicalPlanWithCatalog) subPlan).catalog()).thenReturn(catalog);
    when(((HasIcebergCatalog) catalog).icebergCatalog()).thenReturn(cachingCatalog);
  }

  private static Stream<Arguments> provideCatalogs() {
    return Stream.of(
        arguments(mock(SparkCatalog.class)),
        arguments(mock(SparkSessionCatalog.class)),
        arguments(mock(TestingSparkCatalog.class)),
        arguments(mock(TestingSparkSessionCatalog.class)));
  }

  private static class TestingSparkCatalog extends SparkCatalog {}

  private static class TestingSparkSessionCatalog extends SparkSessionCatalog {}

  @SneakyThrows
  private MetricsReporter getMetricsReporter(BaseMetastoreCatalog catalog) {
    Field field = FieldUtils.getField(catalog.getClass(), "metricsReporter", true);
    return (MetricsReporter) field.get(catalog);
  }

  private static class TestingIcebergCatalog extends BaseMetastoreCatalog {
    @Override
    public String name() {
      return "catalog-name";
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return "";
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {}
  }

  public interface TestingLogicalPlanWithCatalog {
    CatalogPlugin catalog();
  }
}
