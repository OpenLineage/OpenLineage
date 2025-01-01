/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig.VendorsConfig;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.catalyst.plans.logical.BinaryCommand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryCommand;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

/**
 * Declared as a QueryPlanVisitor to be able to inject the InMemoryMetricsReporter into the Iceberg
 * catalog. Registering metric reporter allows enriching OpenLineage events with ScanReport and
 * CommitReport metrics.
 *
 * @param <D>
 */
@Slf4j
public class IcebergMetricsReporterInjector<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {

  private static final String ICEBERG_REPORTER_DISABLED = "iceberg.metricsReporterDisabled";

  public IcebergMetricsReporterInjector(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    // if this is called, then Iceberg classes are on the classpath
    if (Optional.ofNullable(context.getOpenLineageConfig())
        .map(SparkOpenLineageConfig::getVendors)
        .map(VendorsConfig::getAdditionalProperties)
        .map(p -> p.getOrDefault(ICEBERG_REPORTER_DISABLED, "false"))
        .orElse("false")
        .equalsIgnoreCase("true")) {
      log.debug("Iceberg metrics reporter is disabled");
      return false;
    }

    Optional<CatalogPlugin> catalog = getCatalog(plan);

    // check if plan has a catalog field
    if (!catalog.isPresent()) {
      return false;
    }

    // check catalog class starts with org.apache.iceberg
    return catalog.get().getClass().getCanonicalName().startsWith("org.apache.iceberg");
  }

  /**
   * @param plan
   * @return
   */
  private Optional<CatalogPlugin> getCatalog(LogicalPlan plan) {
    if (plan instanceof DataSourceV2Relation) {
      return Optional.ofNullable(((DataSourceV2Relation) plan).catalog().get());
    } else if (plan instanceof DataSourceV2ScanRelation) {
      return Optional.ofNullable(((DataSourceV2ScanRelation) plan).relation().catalog().get());
    }

    Optional<CatalogPlugin> catalog = getCatalogFromCaseClass(plan);
    if (catalog.isPresent()) {
      return catalog;
    }

    if (plan instanceof UnaryCommand) {
      return getCatalogFromCaseClass(((UnaryCommand) plan).child());
    } else if (plan instanceof BinaryCommand) {
      return getCatalogFromCaseClass(((BinaryCommand) plan).left());
    }

    return Optional.empty();
  }

  /**
   * Runs catalog method on LogicalPlan class. This is done through reflection as the case class
   * with catalog property differs across different Spark versions. For example,
   * `ResolvedDBObjectName` is available only for Spark 3.3.
   *
   * <p>For this method, reflection is used for: -
   * org.apache.spark.sql.catalyst.plans.logical.CreateV2Table (3.2.4) -
   * org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect (3.2.4) -
   * org.apache.spark.sql.catalyst.analysis.ResolvedTable (3.2.4) -
   * org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect (3.2.4) -
   * org.apache.spark.sql.catalyst.analysis.ResolvedDBObjectName (3.3.4) -
   * org.apache.spark.sql.catalyst.analysis.ResolvedTable (3.3.4, 3.4.3, 3.5.2) -
   * org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier (3.4.3, 3.5.2)
   *
   * @param plan
   * @return
   */
  private Optional<CatalogPlugin> getCatalogFromCaseClass(LogicalPlan plan) {
    try {
      return Optional.ofNullable(
          (CatalogPlugin) MethodUtils.invokeMethod(plan, "catalog", (Object[]) null));
    } catch (NoSuchMethodException e) {
      // do nothing, don't log
      return Optional.empty();
    } catch (InvocationTargetException | IllegalAccessException e) {
      log.debug("Could not find catalog in plan", e);
      return Optional.empty();
    }
  }

  /**
   * Injects the IcebergMetricsReporter into the Iceberg catalog. Uses reflection as the catalog
   * does not provide public methods to register a metrics reporter after it is initialized.
   *
   * @param x
   * @return
   */
  @Override
  public List<D> apply(LogicalPlan x) {
    SparkCatalog catalog = (SparkCatalog) getCatalog(x).get();
    // hack catalog to inject OpenLineageMetricsReporter
    Catalog icebergCatalog = catalog.icebergCatalog();
    if (icebergCatalog instanceof CachingCatalog) {
      // get root catalog of a caching catalog
      Field catalogField = FieldUtils.getField(icebergCatalog.getClass(), "catalog", true);
      try {
        Catalog rootCatalog = (Catalog) catalogField.get(icebergCatalog);
        if (rootCatalog == null) {
          log.info("Could not inject metrics reporter");
          return Collections.emptyList();
        }

        CatalogMetricsReporterHolder.register(context, rootCatalog);
        return Collections.emptyList();
      } catch (IllegalAccessException e) {
        // do nothing
        log.info("Could not inject metrics reporter", e);
      }
    }

    return Collections.emptyList();
  }
}
