/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.agent.util.DeltaUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.AppendDataDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2ScanRelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.InMemoryRelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MergeIntoCommandEdgeInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MergeIntoCommandEdgeOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MergeIntoCommandInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MergeIntoCommandOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.SparkExtensionV1InputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.SparkExtensionV1OutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.SubqueryAliasInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.SubqueryAliasOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.TableContentChangeDatasetBuilder;
import io.openlineage.spark32.agent.lifecycle.plan.AlterTableCommandDatasetBuilder;
import io.openlineage.spark33.agent.lifecycle.plan.ReplaceIcebergDataDatasetBuilder;
import io.openlineage.spark34.agent.lifecycle.plan.column.CreateReplaceInputDatasetBuilder;
import io.openlineage.spark34.agent.lifecycle.plan.column.DropTableDatasetBuilder;
import io.openlineage.spark35.agent.lifecycle.plan.CreateReplaceOutputDatasetBuilder;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;

@Slf4j
public class Spark35DatasetBuilderFactory extends Spark32DatasetBuilderFactory
    implements DatasetBuilderFactory {

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
            .add(new CreateReplaceInputDatasetBuilder(context))
            .add(new SparkExtensionV1InputDatasetBuilder(context))
            .add(new MergeIntoCommandEdgeInputDatasetBuilder(context))
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
    ImmutableList.Builder builder =
        ImmutableList.<PartialFunction<Object, List<OpenLineage.OutputDataset>>>builder()
            .add(new LogicalRelationDatasetBuilder(context, datasetFactory, false))
            .add(new SaveIntoDataSourceCommandVisitor(context))
            .add(new AppendDataDatasetBuilder(context, datasetFactory))
            .add(new DataSourceV2RelationOutputDatasetBuilder(context, datasetFactory))
            .add(new TableContentChangeDatasetBuilder(context))
            .add(new CreateReplaceOutputDatasetBuilder(context))
            .add(new SparkExtensionV1OutputDatasetBuilder(context))
            .add(new SubqueryAliasOutputDatasetBuilder(context))
            .add(new DropTableDatasetBuilder(context))
            .add(new MergeIntoCommandEdgeOutputDatasetBuilder(context))
            .add(new AlterTableCommandDatasetBuilder(context));

    if (ReplaceIcebergDataDatasetBuilder.hasClasses()) {
      builder.add(new ReplaceIcebergDataDatasetBuilder(context));
    }

    if (DeltaUtils.hasMergeIntoCommandClass()) {
      builder.add(new MergeIntoCommandOutputDatasetBuilder(context));
    }

    return builder.build();
  }
}
