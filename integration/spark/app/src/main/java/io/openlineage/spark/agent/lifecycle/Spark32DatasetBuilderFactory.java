/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
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
import io.openlineage.spark3.agent.lifecycle.plan.TableContentChangeDatasetBuilder;
import io.openlineage.spark32.agent.lifecycle.plan.AlterTableCommandDatasetBuilder;
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
    return ImmutableList.<PartialFunction<Object, List<OpenLineage.InputDataset>>>builder()
        .add(new LogicalRelationDatasetBuilder(context, datasetFactory, true))
        .add(new InMemoryRelationInputDatasetBuilder(context))
        .add(new CommandPlanVisitor(context))
        .add(new DataSourceV2ScanRelationInputDatasetBuilder(context, datasetFactory))
        .add(new DataSourceV2RelationInputDatasetBuilder(context, datasetFactory))
        .build();
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
        .add(getCreateReplaceDatasetBuilder(context))
        .add(new AlterTableCommandDatasetBuilder(context))
        .build();
  }

  private boolean hasAlterTableClass() {
    try {
      Spark32DatasetBuilderFactory.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.catalyst.plans.logical.AlterTable");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
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
}
