/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.DeltaUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.AppendDataDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.CreateReplaceDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2ScanRelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.InMemoryRelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MergeIntoCommandInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MergeIntoCommandOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.SubqueryAliasInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.TableContentChangeDatasetBuilder;
import io.openlineage.spark32.agent.lifecycle.plan.AlterTableCommandDatasetBuilder;
import io.openlineage.spark32.agent.lifecycle.plan.column.MergeIntoDelta11ColumnLineageVisitor;
import io.openlineage.spark32.agent.lifecycle.plan.column.MergeIntoIceberg013ColumnLineageVisitor;
import io.openlineage.spark34.agent.lifecycle.plan.column.MergeIntoDelta24ColumnLineageVisitor;
import io.openlineage.spark34.agent.lifecycle.plan.column.MergeIntoIceberg13ColumnLineageVisitor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

@Slf4j
public class Spark32DatasetBuilderFactory implements DatasetBuilderFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.InputDataset> datasetFactory = DatasetFactory.input(context);
    Builder builder =
        ImmutableList.<PartialFunction<Object, List<InputDataset>>>builder()
            .add(new LogicalRelationDatasetBuilder(context, datasetFactory, true))
            .add(new InMemoryRelationInputDatasetBuilder(context))
            .add(new CommandPlanVisitor(context))
            .add(new DataSourceV2ScanRelationInputDatasetBuilder(context, datasetFactory))
            .add(new SubqueryAliasInputDatasetBuilder(context))
            .add(new DataSourceV2RelationInputDatasetBuilder(context, datasetFactory));

    if (DeltaUtils.hasMergeIntoCommandClass()) {
      builder.add(new MergeIntoCommandInputDatasetBuilder(context));
    }

    return builder.build();
  }

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.OutputDataset> datasetFactory = DatasetFactory.output(context);
    Builder builder =
        ImmutableList.<PartialFunction<Object, List<OutputDataset>>>builder()
            .add(new LogicalRelationDatasetBuilder(context, datasetFactory, false))
            .add(new SaveIntoDataSourceCommandVisitor(context))
            .add(new AppendDataDatasetBuilder(context, datasetFactory))
            .add(new DataSourceV2RelationOutputDatasetBuilder(context, datasetFactory))
            .add(new TableContentChangeDatasetBuilder(context))
            .add(getCreateReplaceDatasetBuilder(context))
            .add(new AlterTableCommandDatasetBuilder(context));

    if (DeltaUtils.hasMergeIntoCommandClass()) {
      builder.add(new MergeIntoCommandOutputDatasetBuilder(context));
    }

    return builder.build();
  }

  private AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> getCreateReplaceDatasetBuilder(
      OpenLineageContext context) {
    // On Databricks platform Spark 3.2 needs to have the version of `CreateReplaceDatasetBuilder`
    // from Spark 3.3. Distinction has to be done by sub object fields
    Method catalogMethod = null;
    try {
      catalogMethod =
          MethodUtils.getAccessibleMethod(
              Class.forName("org.apache.spark.sql.catalyst.plans.logical.ReplaceTable"), "catalog");
    } catch (ClassNotFoundException e) {
      log.error("Could not find `ReplaceTable` class", e);
    }

    if (catalogMethod != null) {
      return new CreateReplaceDatasetBuilder(context);
    } else {
      return new io.openlineage.spark33.agent.lifecycle.plan.CreateReplaceDatasetBuilder(context);
    }
  }

  @Override
  public Collection<ColumnLevelLineageVisitor> getColumnLevelLineageVisitors(
      OpenLineageContext context) {
    ImmutableList.Builder builder = ImmutableList.<ColumnLevelLineageVisitor>builder();

    if (MergeIntoDelta24ColumnLineageVisitor.hasClasses()) {
      builder.add(new MergeIntoDelta24ColumnLineageVisitor(context));
    }

    if (MergeIntoDelta11ColumnLineageVisitor.hasClasses()) {
      builder.add(new MergeIntoDelta11ColumnLineageVisitor(context));
    }

    if (MergeIntoIceberg13ColumnLineageVisitor.hasClasses()) {
      builder.add(new MergeIntoIceberg13ColumnLineageVisitor(context));
    }

    if (MergeIntoIceberg013ColumnLineageVisitor.hasClasses()) {
      builder.add(new MergeIntoIceberg013ColumnLineageVisitor(context));
    }

    return builder.build();
  }
}
