/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.fromMap;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.TableCatalogStorage;
import io.openlineage.spark3.agent.utils.TableCatalogStorage.StoredTableCatalog;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.CreateV2Table;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;

@Slf4j
public class CreateReplaceCatalogExtractor<D extends OpenLineage.Dataset>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, LogicalPlan, D> {

  private static final String CREATE_V2_TABLE =
      "org.apache.spark.sql.catalyst.plans.logical.CreateV2Table";

  public CreateReplaceCatalogExtractor(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof CreateTableAsSelect)
        || (x instanceof ReplaceTable)
        || (x instanceof ReplaceTableAsSelect)
        // Class CreateV2Table was removed in Spark Catalyst 3.3.0. For some reason, it is also
        // missing on Databricks platform when Spark context is in version 3.2.1. This hacky way
        // allows checking for the class also when it is not available on the class path
        || PlanUtils.safeIsInstanceOf(x, CREATE_V2_TABLE);
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
    } else if (PlanUtils.safeIsInstanceOf(x, CREATE_V2_TABLE)) {
      return apply((CreateV2Table) x);
    } else {
      return apply((ReplaceTable) x);
    }
  }

  protected List<D> apply(CreateV2Table plan) {
    TableCatalogStorage.put(
        context.getJobName(), StoredTableCatalog.of(plan.catalog(), fromMap(plan.properties())));
    return Collections.emptyList();
  }

  protected List<D> apply(CreateTableAsSelect plan) {
    TableCatalogStorage.put(
        context.getJobName(), StoredTableCatalog.of(plan.catalog(), fromMap(plan.properties())));
    return Collections.emptyList();
  }

  protected List<D> apply(ReplaceTable plan) {
    TableCatalogStorage.put(
        context.getJobName(), StoredTableCatalog.of(plan.catalog(), fromMap(plan.properties())));
    return Collections.emptyList();
  }

  protected List<D> apply(ReplaceTableAsSelect plan) {
    TableCatalogStorage.put(
        context.getJobName(), StoredTableCatalog.of(plan.catalog(), fromMap(plan.properties())));
    return Collections.emptyList();
  }
}
