/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergMetricsReporterInjectorTest {

  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  IcebergMetricsReporterInjector injector;
  LogicalPlan plan;
  LogicalPlan subPlan;
  SparkCatalog catalog = mock(SparkCatalog.class);
  CachingCatalog cachingCatalog;
  TestingIcebergCatalog icebergCatalog;
  MetricsReporter existingMetricsReporter;

  @BeforeEach
  void setup() {
    injector = new IcebergMetricsReporterInjector(context);
    icebergCatalog = new TestingIcebergCatalog();
    plan =
        mock(LogicalPlan.class, withSettings().extraInterfaces(TestingLogicalPlanWithName.class));

    subPlan =
        mock(
            LogicalPlan.class, withSettings().extraInterfaces(TestingLogicalPlanWithCatalog.class));
    when(((TestingLogicalPlanWithCatalog) subPlan).catalog()).thenReturn(catalog);
    when(((TestingLogicalPlanWithName) plan).name()).thenReturn(subPlan);

    when(context
            .getOpenLineageConfig()
            .getVendors()
            .getAdditionalProperties()
            .getOrDefault("iceberg.metricsReporterDisabled", "false"))
        .thenReturn("false");

    cachingCatalog = (CachingCatalog) CachingCatalog.wrap(icebergCatalog);
    when(catalog.icebergCatalog()).thenReturn(cachingCatalog);
    CatalogMetricsReporterHolder.getInstance().clear();
  }

  @Test
  void testIsDefinedForIcebergCatalog() {
    assertThat(injector.isDefinedAt(mock(LogicalPlan.class))).isFalse();
    assertThat(injector.isDefinedAt(plan)).isTrue();

    LogicalPlan planWithCatalogMethod =
        mock(
            LogicalPlan.class, withSettings().extraInterfaces(TestingLogicalPlanWithCatalog.class));
    when(((TestingLogicalPlanWithCatalog) planWithCatalogMethod).catalog()).thenReturn(catalog);
    assertThat(injector.isDefinedAt(planWithCatalogMethod)).isTrue();
  }

  @Test
  void testIsDefinedWhenMetricsReporterDisabled() {
    when(context
            .getOpenLineageConfig()
            .getVendors()
            .getAdditionalProperties()
            .getOrDefault("iceberg.metricsReporterDisabled", "false"))
        .thenReturn("true");
    assertThat(injector.isDefinedAt(plan)).isFalse();
  }

  @Test
  void testIsDefinedForNonIcebergCatalog() {
    subPlan =
        mock(
            LogicalPlan.class, withSettings().extraInterfaces(TestingLogicalPlanWithCatalog.class));
    when(((TestingLogicalPlanWithCatalog) subPlan).catalog())
        .thenReturn(mock(TableCatalog.class)); // not an instance of SparkCatalog
    when(((TestingLogicalPlanWithName) plan).name()).thenReturn(subPlan);

    assertThat(injector.isDefinedAt(plan)).isFalse();
  }

  @Test
  void testApplyInjectsMetricsReporter() {
    icebergCatalog.metricsReporter = null;
    injector.apply(plan);

    assertThat(icebergCatalog.metricsReporter)
        .isEqualTo(CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name"));
  }

  @Test
  void testApplyInjectsMetricReporterWith() {
    icebergCatalog.metricsReporter = existingMetricsReporter;

    injector.apply(plan);

    assertThat(
            CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name").getDelegate())
        .isEqualTo(existingMetricsReporter);

    assertThat(icebergCatalog.metricsReporter)
        .isEqualTo(CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name"));
  }

  private static class TestingIcebergCatalog implements Catalog {

    public MetricsReporter metricsReporter;

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

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) {
      return null;
    }

    public String name() {
      return "catalog-name";
    }
  }

  public interface TestingLogicalPlanWithName {
    LogicalPlan name();
  }

  public interface TestingLogicalPlanWithCatalog {
    TableCatalog catalog();
  }
}
