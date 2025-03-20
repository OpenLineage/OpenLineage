/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder.VENDOR_CONTEXT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.VendorsContext;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.sql.catalyst.plans.logical.BinaryCommand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryCommand;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergMetricsReporterInjectorTest {

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

    when(context
            .getOpenLineageConfig()
            .getVendors()
            .getAdditionalProperties()
            .getOrDefault("iceberg.metricsReporterDisabled", "false"))
        .thenReturn("false");
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

  @ParameterizedTest
  @MethodSource("provideCatalogs")
  void testIsDefinedWhenMetricsReporterDisabled(CatalogPlugin catalog) {
    setupCatalog(catalog);

    when(context
            .getOpenLineageConfig()
            .getVendors()
            .getAdditionalProperties()
            .getOrDefault("iceberg.metricsReporterDisabled", "false"))
        .thenReturn("true");
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

  private void setupCatalog(CatalogPlugin catalog) {
    when(((TestingLogicalPlanWithCatalog) subPlan).catalog()).thenReturn(catalog);
    when(((HasIcebergCatalog) catalog).icebergCatalog()).thenReturn(cachingCatalog);
  }

  private static Stream<Arguments> provideCatalogs() {
    return Stream.of(
        arguments(mock(SparkCatalog.class)), arguments(mock(SparkSessionCatalog.class)));
  }

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
