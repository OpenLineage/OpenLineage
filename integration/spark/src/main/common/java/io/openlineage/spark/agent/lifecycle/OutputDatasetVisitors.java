package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.AppendDataVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveTableVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetWithMetadataVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Constructs a list of valid {@link LogicalPlan} visitors that can extract an output {@link
 * OpenLineage.Dataset}. Checks the classpath for classes that are not bundled with Spark to avoid
 * {@link ClassNotFoundException}s during plan traversal.
 */
class OutputDatasetVisitors
    implements Supplier<List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>>> {
  private final SQLContext sqlContext;
  private final List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> datasetProviders;

  public OutputDatasetVisitors(
      SQLContext sqlContext,
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> datasetProviders) {
    this.sqlContext = sqlContext;
    this.datasetProviders = datasetProviders;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> get() {
    List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> list = new ArrayList<>();

    list.add(new OutputDatasetWithMetadataVisitor(new InsertIntoDataSourceDirVisitor()));
    list.add(
        new OutputDatasetWithMetadataVisitor(new InsertIntoDataSourceVisitor(datasetProviders)));
    list.add(new OutputDatasetWithMetadataVisitor(new InsertIntoHadoopFsRelationVisitor()));
    list.add(
        new OutputDatasetWithMetadataVisitor(
            new SaveIntoDataSourceCommandVisitor(sqlContext, datasetProviders)));
    //list.add(new OutputDatasetVisitor(new DatasetSourceVisitor()));
    list.add(new OutputDatasetVisitor(new AppendDataVisitor(datasetProviders)));
    list.add(new OutputDatasetVisitor(new InsertIntoDirVisitor(sqlContext)));
    list.add(
        new OutputDatasetWithMetadataVisitor(
            new InsertIntoHiveTableVisitor(sqlContext.sparkContext())));
    list.add(new OutputDatasetVisitor(new InsertIntoHiveDirVisitor()));
    return list;
  }
}
