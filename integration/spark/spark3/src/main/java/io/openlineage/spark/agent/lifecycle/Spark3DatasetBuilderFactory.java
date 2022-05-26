package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.AlterTableDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.AppendDataDatasetBuilder3;
import io.openlineage.spark3.agent.lifecycle.plan.CreateReplaceDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationOutputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2ScanRelationInputDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.LogicalRelationDatasetBuilder3;
import io.openlineage.spark3.agent.lifecycle.plan.TableContentChangeDatasetBuilder;
import java.util.Collection;
import java.util.List;
import scala.PartialFunction;

public class Spark3DatasetBuilderFactory implements DatasetBuilderFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.InputDataset> datasetFactory = DatasetFactory.input(context);
    return ImmutableList.<PartialFunction<Object, List<OpenLineage.InputDataset>>>builder()
        .add(new LogicalRelationDatasetBuilder3(context, datasetFactory, true))
        .add(new CommandPlanVisitor(context))
        .add(new DataSourceV2ScanRelationInputDatasetBuilder(context, datasetFactory))
        .add(new DataSourceV2RelationInputDatasetBuilder(context, datasetFactory))
        .build();
  }

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.OutputDataset> datasetFactory = DatasetFactory.output(context);
    ImmutableList.Builder builder =
        ImmutableList.<PartialFunction<Object, List<OpenLineage.OutputDataset>>>builder()
            .add(new LogicalRelationDatasetBuilder3(context, datasetFactory, false))
            .add(new SaveIntoDataSourceCommandVisitor(context))
            .add(new AppendDataDatasetBuilder3(context, datasetFactory))
            .add(new DataSourceV2RelationOutputDatasetBuilder(context, datasetFactory))
            .add(new TableContentChangeDatasetBuilder(context))
            .add(new CreateReplaceDatasetBuilder(context));

    if (hasAlterTableClass()) {
      builder.add(new AlterTableDatasetBuilder(context));
    }

    return builder.build();
  }

  private boolean hasAlterTableClass() {
    try {
      Spark3DatasetBuilderFactory.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.catalyst.plans.logical.AlterTable");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }
}
