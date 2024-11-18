/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.TableCatalog;

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

    Optional<TableCatalog> catalog = getCatalog(plan);

    // check if plan has a catalog field
    if (!catalog.isPresent()) {
      return false;
    }

    // check catalog class starts with org.apache.iceberg
    return catalog.get().getClass().getCanonicalName().startsWith("org.apache.iceberg");
  }

  private Optional<TableCatalog> getCatalog(LogicalPlan plan) {
    try {
      if (MethodUtils.getAccessibleMethod(plan.getClass(), "name") == null) {
        // try calling catalog method directly
        return Optional.ofNullable(
            (TableCatalog) MethodUtils.invokeMethod(plan, "catalog", (Object[]) null));
      } else {
        // call catalog method on name LogicalPlan
        LogicalPlan namePlan =
            (LogicalPlan) MethodUtils.invokeMethod(plan, "name", (Object[]) null);

        if (MethodUtils.getAccessibleMethod(namePlan.getClass(), "catalog") == null) {
          return Optional.empty();
        }

        return Optional.ofNullable(
            (TableCatalog) MethodUtils.invokeMethod(namePlan, "catalog", (Object[]) null));
      }
    } catch (NoSuchMethodException e) {
      // do nothing, don't log
      return Optional.empty();
    } catch (InvocationTargetException | IllegalAccessException e) {
      log.debug("Could not find catalog in plan", e);
      return Optional.empty();
    }
  }

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

        CatalogMetricsReporterHolder.getInstance().register(rootCatalog);
        return Collections.emptyList();
      } catch (IllegalAccessException e) {
        // do nothing
        log.info("Could not inject metrics reporter", e);
      }
    }

    return Collections.emptyList();
  }
}
