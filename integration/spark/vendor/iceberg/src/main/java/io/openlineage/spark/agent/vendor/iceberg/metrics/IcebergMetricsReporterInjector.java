/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper.BaseTableWrapper;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig.VendorsConfig;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.BinaryCommand;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryCommand;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import scala.Option;

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

  private static final List<String> WRITE_TARGET_METHODS =
      Arrays.asList("table", "originalTable", "relation");
  private static final int MAX_TABLE_UNWRAP_DEPTH = 3;

  public IcebergMetricsReporterInjector(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    // if this is called, then Iceberg classes are on the classpath
    if (Optional.ofNullable(context.getOpenLineageConfig())
        .map(SparkOpenLineageConfig::getVendors)
        .map(VendorsConfig::getConfig)
        .flatMap(p -> Optional.ofNullable(p.get("iceberg")))
        .map(VendorsConfig.VendorConfig::getMetricsReporterDisabled)
        .orElse(false)) {
      log.debug("Iceberg metrics reporter is disabled");
      return false;
    }

    return getCatalog(plan).filter(this::isIcebergCatalog).isPresent()
        || getRelations(plan).stream()
            .anyMatch(
                relation ->
                    ScalaConversionUtils.asJavaOptional(relation.catalog())
                        .filter(this::isIcebergCatalog)
                        .isPresent());
  }

  private boolean isIcebergCatalog(CatalogPlugin catalog) {
    return catalog instanceof SparkCatalog || catalog instanceof SparkSessionCatalog;
  }

  /**
   * Returns the relations whose tables can report metrics for this node: the node itself, the
   * relation of a scan, and the write target of a command. Write targets such as {@code
   * V2WriteCommand.table()}, {@code RowLevelWrite.originalTable()} and {@code
   * WriteToDataSourceV2.relation()} are not children of the node, so a plan traversal does not
   * visit them. They are read by reflection as the available methods differ across Spark versions.
   */
  private List<DataSourceV2Relation> getRelations(LogicalPlan plan) {
    if (plan instanceof DataSourceV2Relation) {
      return Collections.singletonList((DataSourceV2Relation) plan);
    } else if (plan instanceof DataSourceV2ScanRelation) {
      return Collections.singletonList(((DataSourceV2ScanRelation) plan).relation());
    } else if (!(plan instanceof Command) && !(plan instanceof WriteToDataSourceV2)) {
      return Collections.emptyList();
    }

    List<DataSourceV2Relation> relations = new ArrayList<>();
    for (String method : WRITE_TARGET_METHODS) {
      Object target = unwrapOption(invokeNoArgMethod(plan, method));
      if (target instanceof DataSourceV2Relation) {
        relations.add((DataSourceV2Relation) target);
      }
    }
    return relations;
  }

  private static Object unwrapOption(Object value) {
    if (!(value instanceof Option)) {
      return value;
    }
    Option<?> option = (Option<?>) value;
    if (option.isDefined()) {
      return option.get();
    }
    return null;
  }

  private static Object invokeNoArgMethod(Object object, String method) {
    try {
      return MethodUtils.invokeMethod(object, method);
    } catch (NoSuchMethodException e) {
      // do nothing, don't log
      return null;
    } catch (InvocationTargetException | IllegalAccessException | RuntimeException e) {
      log.debug("Could not call {} on {}", method, object.getClass().getName(), e);
      return null;
    }
  }

  /**
   * @param plan
   * @return
   */
  private Optional<CatalogPlugin> getCatalog(LogicalPlan plan) {
    if (plan instanceof DataSourceV2Relation) {
      return ScalaConversionUtils.asJavaOptional(((DataSourceV2Relation) plan).catalog());
    } else if (plan instanceof DataSourceV2ScanRelation) {
      return ScalaConversionUtils.asJavaOptional(
          ((DataSourceV2ScanRelation) plan).relation().catalog());
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
   * org.apache.spark.sql.catalyst.analysis.ResolvedTable (3.3.4, 3.4.3, 3.5.4) -
   * org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier (3.4.3, 3.5.4)
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
   * <p>Iceberg tables keep the reporter their catalog had when the table object was created, and
   * Spark loads tables during analysis, before OpenLineage handles the query. The reporter is
   * therefore also attached to the Iceberg tables referenced by the node, so that their commits are
   * reported.
   *
   * @param x
   * @return
   */
  @Override
  public List<D> apply(LogicalPlan x) {
    // hack catalog to inject OpenLineageMetricsReporter
    getCatalog(x).ifPresent(this::registerCatalog);

    for (DataSourceV2Relation relation : getRelations(x)) {
      ScalaConversionUtils.asJavaOptional(relation.catalog())
          .flatMap(this::registerCatalog)
          .ifPresent(
              reporter ->
                  getIcebergTable(relation.table())
                      .ifPresent(table -> BaseTableWrapper.attach(table, reporter)));
    }

    return Collections.emptyList();
  }

  private Optional<OpenLineageMetricsReporter> registerCatalog(CatalogPlugin catalogPlugin) {
    Optional<Catalog> catalog = getIcebergCatalog(catalogPlugin);
    if (!catalog.isPresent()) {
      return Optional.empty();
    }

    Catalog icebergCatalog = catalog.get();
    if (icebergCatalog instanceof CachingCatalog) {
      // get root catalog of a caching catalog
      Field catalogField = FieldUtils.getField(icebergCatalog.getClass(), "catalog", true);
      try {
        Catalog rootCatalog = (Catalog) catalogField.get(icebergCatalog);
        if (rootCatalog == null) {
          log.info("Could not inject metrics reporter");
          return Optional.empty();
        }

        return CatalogMetricsReporterHolder.register(context, rootCatalog);
      } catch (IllegalAccessException e) {
        // do nothing
        log.info("Could not inject metrics reporter", e);
      }
    }

    return Optional.empty();
  }

  /**
   * Returns the Iceberg table behind a Spark table. {@code SparkTable.table()} returns the Iceberg
   * table. Row-level operations wrap the {@code SparkTable} in a {@code RowLevelOperationTable},
   * whose {@code table()} returns the wrapped table.
   */
  private Optional<Table> getIcebergTable(Object sparkTable) {
    Object table = sparkTable;
    for (int depth = 0; depth < MAX_TABLE_UNWRAP_DEPTH && table != null; depth++) {
      if (table instanceof Table) {
        return Optional.of((Table) table);
      }
      table = invokeNoArgMethod(table, "table");
    }
    return Optional.empty();
  }

  private Optional<Catalog> getIcebergCatalog(CatalogPlugin catalog) {
    if (catalog instanceof SparkCatalog) {
      return Optional.ofNullable(((SparkCatalog) catalog).icebergCatalog());
    }
    if (catalog instanceof SparkSessionCatalog) {
      return Optional.ofNullable(((SparkSessionCatalog<?>) catalog).icebergCatalog());
    }
    return Optional.empty();
  }
}
