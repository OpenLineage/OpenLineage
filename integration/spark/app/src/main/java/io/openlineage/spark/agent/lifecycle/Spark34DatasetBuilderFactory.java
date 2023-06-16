/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.AppendDataDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MapPartitionsDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.MergeIntoCommandOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.TableContentChangeDatasetBuilder;
import io.openlineage.spark32.agent.lifecycle.plan.AlterTableCommandDatasetBuilder;
import io.openlineage.spark33.agent.lifecycle.plan.CreateReplaceDatasetBuilder;
import io.openlineage.spark33.agent.lifecycle.plan.ReplaceIcebergDataDatasetBuilder;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;

@Slf4j
public class Spark34DatasetBuilderFactory extends Spark32DatasetBuilderFactory
    implements DatasetBuilderFactory {

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
            .add(new MapPartitionsDatasetBuilder(context))
            .add(new CreateReplaceDatasetBuilder(context))
            .add(new AlterTableCommandDatasetBuilder(context));

    if (ReplaceIcebergDataDatasetBuilder.hasClasses()) {
      builder.add(new ReplaceIcebergDataDatasetBuilder(context));
    }

    if (MergeIntoCommandOutputDatasetBuilder.hasClasses()) {
      builder.add(new MergeIntoCommandOutputDatasetBuilder(context));
    }

    return builder.build();
  }
}
