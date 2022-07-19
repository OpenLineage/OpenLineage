/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.CreateTable;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateReplaceDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public CreateReplaceDatasetBuilder(OpenLineageContext context) {
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
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan plan) {
    if (plan instanceof CreateTableAsSelect) {
      return apply(event, (CreateTableAsSelect) plan);
    } else if (plan instanceof ReplaceTableAsSelect) {
      return apply(event, (ReplaceTableAsSelect) plan);
    } else if (plan instanceof CreateTable) {
      return apply(event, (CreateTable) plan);
    } else {
      return apply(event, (ReplaceTable) plan);
    }
  }

  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, CreateTable plan) {
    return callCatalogMethod(plan.name())
        .map(
            catalogPlugin ->
                apply(
                    event,
                    catalogPlugin,
                    ScalaConversionUtils.<String, String>fromMap(plan.tableSpec().properties()),
                    plan.tableName(),
                    plan.tableSchema(),
                    LifecycleStateChange.CREATE))
        .orElse(Collections.emptyList());
  }

  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, CreateTableAsSelect plan) {
    return callCatalogMethod(plan.name())
        .map(
            catalogPlugin ->
                apply(
                    event,
                    catalogPlugin,
                    ScalaConversionUtils.<String, String>fromMap(plan.tableSpec().properties()),
                    plan.tableName(),
                    plan.tableSchema(),
                    LifecycleStateChange.CREATE))
        .orElse(Collections.emptyList());
  }

  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, ReplaceTable plan) {
    return callCatalogMethod(plan.name())
        .map(
            catalogPlugin ->
                apply(
                    event,
                    catalogPlugin,
                    ScalaConversionUtils.<String, String>fromMap(plan.tableSpec().properties()),
                    plan.tableName(),
                    plan.tableSchema(),
                    LifecycleStateChange.OVERWRITE))
        .orElse(Collections.emptyList());
  }

  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, ReplaceTableAsSelect plan) {
    return callCatalogMethod(plan.name())
        .map(
            catalogPlugin ->
                apply(
                    event,
                    catalogPlugin,
                    ScalaConversionUtils.<String, String>fromMap(plan.tableSpec().properties()),
                    plan.tableName(),
                    plan.tableSchema(),
                    LifecycleStateChange.OVERWRITE))
        .orElse(Collections.emptyList());
  }

  private Optional<TableCatalog> callCatalogMethod(LogicalPlan plan) {
    try {
      return Optional.of((TableCatalog) MethodUtils.invokeMethod(plan, "catalog", null));
    } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      log.error("Could not obtain catalog plugin", e);
      return Optional.empty();
    }
  }

  private List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event,
      TableCatalog catalog,
      Map<String, String> tableProperties,
      Identifier identifier,
      StructType schema,
      LifecycleStateChange lifecycleStateChange) {

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, catalog, identifier, tableProperties);

    if (!di.isPresent()) {
      return Collections.emptyList();
    }

    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.DatasetFacetsBuilder builder =
        openLineage
            .newDatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(openLineage, schema))
            .lifecycleStateChange(
                openLineage.newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null))
            .dataSource(PlanUtils.datasourceFacet(openLineage, di.get().getNamespace()));

    if (includeDatasetVersion(event)) {
      Optional<String> datasetVersion =
          CatalogUtils3.getDatasetVersion(context, catalog, identifier, tableProperties);
      datasetVersion.ifPresent(
          version -> builder.version(openLineage.newDatasetVersionDatasetFacet(version)));
    }

    CatalogUtils3.getStorageDatasetFacet(context, catalog, tableProperties)
        .map(storageDatasetFacet -> builder.storage(storageDatasetFacet));
    return Collections.singletonList(outputDataset().getDataset(di.get(), builder));
  }
}
