/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class AppendDataDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<AppendData> {

  private final DatasetFactory<OpenLineage.OutputDataset> factory;

  public AppendDataDatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.OutputDataset> factory) {
    super(context, false);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof AppendData;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, AppendData x) {
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    LogicalPlan table = (LogicalPlan) x.table();
    Optional<DataSourceV2Relation> relation = resolveDataSourceV2Relation(table);
    if (relation.isPresent()) {
      // Run query plan visitors (for example, Iceberg metrics reporter injection) before building
      // output datasets directly from the target relation.
      delegate(table, event);
      return DataSourceV2RelationDatasetExtractor.extract(
          factory, context, relation.get(), includeDatasetVersion(event));
    }

    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            table,
            ScalaConversionUtils.toScalaFn(
                (lp) -> Collections.<OpenLineage.OutputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }

  private Optional<DataSourceV2Relation> resolveDataSourceV2Relation(LogicalPlan table) {
    if (table instanceof DataSourceV2Relation) {
      return Optional.of((DataSourceV2Relation) table);
    }
    if (table instanceof DataSourceV2ScanRelation) {
      return Optional.of(((DataSourceV2ScanRelation) table).relation());
    }
    if (table instanceof SubqueryAlias) {
      return resolveDataSourceV2Relation(((SubqueryAlias) table).child());
    }
    return Optional.empty();
  }

  @Override
  public Optional<String> jobNameSuffix(AppendData plan) {
    if (plan.table() instanceof DataSourceV2Relation) {
      return new DataSourceV2RelationOutputDatasetBuilder(context, factory)
          .jobNameSuffix((DataSourceV2Relation) (plan.table()));
    } else {
      return Optional.ofNullable(plan.table()).map(t -> t.name());
    }
  }
}
