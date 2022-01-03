package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.CreateReplaceVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.CreateTableLikeCommandVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.DataSourceV2RelationVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.TableContentChangeVisitor;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

class Spark3VisitorFactoryImpl extends BaseVisitorFactory {

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      OpenLineageContext context) {
    DatasetFactory<OutputDataset> outputFactory = DatasetFactory.output(context.getOpenLineage());
    return ImmutableList.<PartialFunction<LogicalPlan, List<OutputDataset>>>builder()
        .addAll(super.getOutputVisitors(context))
        .add(new CreateReplaceVisitor(context))
        .add(new DataSourceV2RelationVisitor(context, outputFactory))
        .add(new TableContentChangeVisitor(context))
        .add(new CreateTableLikeCommandVisitor(context))
        .build();
  }

  public <D extends Dataset> List<PartialFunction<LogicalPlan, List<D>>> getCommonVisitors(
      OpenLineageContext context, DatasetFactory<D> factory) {
    return super.getBaseCommonVisitors(context, factory);
  }
}
