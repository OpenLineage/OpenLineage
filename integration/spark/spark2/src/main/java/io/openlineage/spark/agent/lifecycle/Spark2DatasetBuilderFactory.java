package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collection;
import java.util.List;
import scala.PartialFunction;

public class Spark2DatasetBuilderFactory implements DatasetBuilderFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
      OpenLineageContext context) {
    return ImmutableList.<PartialFunction<Object, List<OpenLineage.InputDataset>>>builder()
        .add(new LogicalRelationDatasetBuilder(context, DatasetFactory.input(context), true))
        .add(new CommandPlanVisitor(context))
        .build();
  }

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
      OpenLineageContext context) {
    return ImmutableList.<PartialFunction<Object, List<OpenLineage.OutputDataset>>>builder()
        .add(new LogicalRelationDatasetBuilder(context, DatasetFactory.output(context), false))
        .add(new SaveIntoDataSourceCommandVisitor(context))
        .build();
  }
}
