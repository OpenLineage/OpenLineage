/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.fromMap;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.TableCatalogStorage;
import io.openlineage.spark3.agent.utils.TableCatalogStorage.StoredTableCatalog;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.CreateTable;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class CreateReplaceCatalogExtractor<D extends OpenLineage.Dataset>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, LogicalPlan, D> {

  public CreateReplaceCatalogExtractor(OpenLineageContext context) {
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
  public List<D> apply(LogicalPlan logicalPlan) {
    // intentionally unimplemented
    throw new UnsupportedOperationException("apply(LogicalPlan) is not implemented");
  }

  @Override
  public List<D> apply(SparkListenerEvent event, LogicalPlan x) {
    if (x instanceof CreateTableAsSelect) {
      return apply((CreateTableAsSelect) x);
    } else if (x instanceof ReplaceTableAsSelect) {
      return apply((ReplaceTableAsSelect) x);
    } else if (x instanceof CreateTable) {
      return apply((CreateTable) x);
    } else {
      return apply((ReplaceTable) x);
    }
  }

  protected List<D> apply(CreateTable plan) {
    callCatalogMethod(plan.name())
        .ifPresent(
            catalogPlugin ->
                TableCatalogStorage.put(
                    context.getJobName(),
                    StoredTableCatalog.of(catalogPlugin, fromMap(plan.tableSpec().properties()))));
    return Collections.emptyList();
  }

  protected List<D> apply(CreateTableAsSelect plan) {
    Map<String, String> tableProperties = fromMap(plan.tableSpec().properties());
    tableProperties.putAll(fromMap(plan.writeOptions()));
    callCatalogMethod(plan.name())
        .ifPresent(
            catalogPlugin ->
                TableCatalogStorage.put(
                    context.getJobName(), StoredTableCatalog.of(catalogPlugin, tableProperties)));
    return Collections.emptyList();
  }

  protected List<D> apply(ReplaceTable plan) {
    callCatalogMethod(plan.name())
        .ifPresent(
            catalogPlugin ->
                TableCatalogStorage.put(
                    context.getJobName(),
                    StoredTableCatalog.of(catalogPlugin, fromMap(plan.tableSpec().properties()))));
    return Collections.emptyList();
  }

  protected List<D> apply(ReplaceTableAsSelect plan) {
    Map<String, String> tableProperties = fromMap(plan.tableSpec().properties());
    tableProperties.putAll(fromMap(plan.writeOptions()));
    callCatalogMethod(plan.name())
        .ifPresent(
            catalogPlugin ->
                TableCatalogStorage.put(
                    context.getJobName(), StoredTableCatalog.of(catalogPlugin, tableProperties)));
    return Collections.emptyList();
  }

  private Optional<TableCatalog> callCatalogMethod(LogicalPlan plan) {
    try {
      return Optional.of((TableCatalog) MethodUtils.invokeMethod(plan, "catalog", (Object[]) null));
    } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      log.error("Could not obtain catalog plugin", e);
      return Optional.empty();
    }
  }
}
