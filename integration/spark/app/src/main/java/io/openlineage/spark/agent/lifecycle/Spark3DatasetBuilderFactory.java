/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.DeltaUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.AlterTableDatasetBuilder;
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
import io.openlineage.spark32.agent.lifecycle.plan.column.MergeIntoDelta11ColumnLineageVisitor;
import io.openlineage.spark32.agent.lifecycle.plan.column.MergeIntoIceberg013ColumnLineageVisitor;
import io.openlineage.spark34.agent.lifecycle.plan.column.MergeIntoDelta24ColumnLineageVisitor;
import io.openlineage.spark34.agent.lifecycle.plan.column.MergeIntoIceberg13ColumnLineageVisitor;
import java.util.Collection;
import java.util.List;
import scala.PartialFunction;

public class Spark3DatasetBuilderFactory implements DatasetBuilderFactory {
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
            .add(new DataSourceV2RelationInputDatasetBuilder(context, datasetFactory))
            .add(new SubqueryAliasInputDatasetBuilder(context))
            .add(new MergeIntoCommandInputDatasetBuilder(context));

    if (DeltaUtils.hasMergeIntoCommandClass()) {
      builder.add(new MergeIntoCommandInputDatasetBuilder(context));
    }

    return builder.build();
  }

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.OutputDataset> datasetFactory = DatasetFactory.output(context);
    return ImmutableList.<PartialFunction<Object, List<OpenLineage.OutputDataset>>>builder()
        .add(new LogicalRelationDatasetBuilder(context, datasetFactory, false))
        .add(new SaveIntoDataSourceCommandVisitor(context))
        .add(new AppendDataDatasetBuilder(context, datasetFactory))
        .add(new DataSourceV2RelationOutputDatasetBuilder(context, datasetFactory))
        .add(new TableContentChangeDatasetBuilder(context))
        .add(new MergeIntoCommandOutputDatasetBuilder(context))
        .add(new CreateReplaceDatasetBuilder(context))
        .add(new AlterTableDatasetBuilder(context))
        .build();
  }

  @Override
  public Collection<ColumnLevelLineageVisitor> getColumnLevelLineageVisitors(
      OpenLineageContext context) {
    Builder<ColumnLevelLineageVisitor> builder = ImmutableList.<ColumnLevelLineageVisitor>builder();

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
