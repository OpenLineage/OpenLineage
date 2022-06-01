package io.openlineage.spark.shared.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.shared.agent.lifecycle.Rdds;
import io.openlineage.spark.shared.api.DatasetFactory;
import io.openlineage.spark.shared.api.OpenLineageContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ExternalRDD;

import java.util.List;

/** {@link RDD} node visitor for {@link ExternalRDD}s. */
public class ExternalRDDVisitor extends AbstractRDDNodeVisitor<ExternalRDD<?>, InputDataset> {

  public ExternalRDDVisitor(OpenLineageContext context) {
    super(context, DatasetFactory.input(context));
  }

  @Override
  public List<InputDataset> apply(LogicalPlan x) {
    ExternalRDD externalRDD = (ExternalRDD) x;
    List<RDD<?>> fileRdds = Rdds.findFileLikeRdds(externalRDD.rdd());
    return findInputDatasets(fileRdds, externalRDD.schema());
  }
}
