/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark35.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import io.openlineage.spark3.agent.utils.V2CreateTablePlanUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.plans.logical.CreateTable;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written. Although the builder is within spark35 package, it's
 * added as a dataset builder for Spark 3.4 on Databricks runtime.
 *
 * <p>Plan members are read through {@link V2CreateTablePlanUtils} because Databricks runtimes
 * change the signatures of these nodes, and a {@link NoSuchMethodError} here would leave the event
 * without any output dataset.
 */
@Slf4j
public class CreateReplaceOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public CreateReplaceOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof CreateTableAsSelect)
        || (x instanceof ReplaceTable)
        || (x instanceof ReplaceTableAsSelect)
        || (x instanceof CreateTable);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return (event instanceof SparkListenerSQLExecutionEnd || event instanceof SparkListenerJobEnd);
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan plan) {
    Optional<TableCatalog> catalog = V2CreateTablePlanUtils.catalog(plan);
    Optional<Identifier> identifier = V2CreateTablePlanUtils.identifier(plan);

    if (!catalog.isPresent() || !identifier.isPresent()) {
      log.warn(
          "Could not obtain catalog and identifier from {}", plan.getClass().getCanonicalName());
      return Collections.emptyList();
    }

    return apply(
        event,
        catalog.get(),
        V2CreateTablePlanUtils.properties(plan),
        identifier.get(),
        V2CreateTablePlanUtils.schema(plan),
        lifecycleStateChange(plan));
  }

  private static LifecycleStateChange lifecycleStateChange(LogicalPlan plan) {
    return (plan instanceof ReplaceTable || plan instanceof ReplaceTableAsSelect)
        ? LifecycleStateChange.OVERWRITE
        : LifecycleStateChange.CREATE;
  }

  private List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event,
      TableCatalog catalog,
      Map<String, String> tableProperties,
      Identifier identifier,
      StructType schema,
      LifecycleStateChange lifecycleStateChange) {

    Optional<DatasetIdentifier> di = datasetIdentifier(catalog, identifier, tableProperties);

    if (!di.isPresent()) {
      return Collections.emptyList();
    }

    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder =
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(di.get())
            .schema(schema)
            .lifecycleStateChange(lifecycleStateChange);

    // A catalog that cannot describe the table must not cost the output dataset itself
    try {
      if (includeDatasetVersion(event)) {
        CatalogUtils.getDatasetVersion(context, catalog, identifier, tableProperties)
            .ifPresent(sparkBuilder::version);
      }
      CatalogUtils.addStorageAndCatalogFacets(
          context, catalog, tableProperties, sparkBuilder.getInner());
    } catch (Exception | NoSuchMethodError | NoClassDefFoundError e) {
      log.warn("Could not add catalog facets of table {}", identifier, e);
    }

    return Collections.singletonList(sparkBuilder.build());
  }

  private Optional<DatasetIdentifier> datasetIdentifier(
      TableCatalog catalog, Identifier identifier, Map<String, String> tableProperties) {
    Optional<DatasetIdentifier> di;
    try {
      di = PlanUtils3.getDatasetIdentifier(context, catalog, identifier, tableProperties);
    } catch (Exception | NoSuchMethodError | NoClassDefFoundError e) {
      log.warn("Could not resolve dataset identifier of table {}", identifier, e);
      di = Optional.empty();
    }

    if (di.isPresent()) {
      return di;
    }

    Optional<DatasetIdentifier> fallback = unityCatalogIdentifier(catalog, identifier);
    if (!fallback.isPresent()) {
      log.warn(
          "No dataset identifier resolved for table {} of catalog {}",
          identifier,
          catalog.getClass().getCanonicalName());
    }
    return fallback;
  }

  /**
   * Unity Catalog managed tables have no location available at the time the create command is
   * built, and the session catalog cannot resolve a default path for a Unity Catalog namespace. In
   * that case the table is identified by its {@code catalog.schema.table} name, which is the same
   * name attached as a symlink when the location is known, so both forms refer to the same table.
   */
  private Optional<DatasetIdentifier> unityCatalogIdentifier(
      TableCatalog catalog, Identifier identifier) {
    boolean unityCatalogEnabled =
        context
            .getSparkContext()
            .map(SparkContext::getConf)
            .map(DatabricksUtils::isDatabricksUnityCatalogEnabled)
            .orElse(false);

    if (!unityCatalogEnabled) {
      return Optional.empty();
    }

    String name = DatabricksUtils.qualifiedUnityCatalogTableName(catalog, identifier);
    log.warn(
        "Could not resolve the location of Unity Catalog table {}, falling back to its qualified name",
        name);
    return Optional.of(
        new DatasetIdentifier(name, DatabricksUtils.UNITY_CATALOG_SYMLINK_NAMESPACE));
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    if (!this.isDefinedAtLogicalPlan(plan)) {
      return Optional.empty();
    }

    return V2CreateTablePlanUtils.identifier(plan).map(this::identToSuffix);
  }
}
