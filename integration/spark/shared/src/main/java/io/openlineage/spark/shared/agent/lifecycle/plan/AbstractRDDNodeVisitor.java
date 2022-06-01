package io.openlineage.spark.shared.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.agent.util.PlanUtils;
import io.openlineage.spark.shared.api.DatasetFactory;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark.shared.api.QueryPlanVisitor;
import lombok.NonNull;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Base node visitor for classes that extract {@link org.apache.spark.sql.Dataset}s from {@link
 * RDD}s. File-based {@link RDD}s, including {@link HadoopRDD} and {@link FileScanRDD} are handled
 * by the {@link #findInputDatasets(List, StructType)} method.
 *
 * @param <T>
 * @param <D>
 */
public abstract class AbstractRDDNodeVisitor<T extends LogicalPlan, D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<T, D> {

  protected final DatasetFactory<D> datasetFactory;

  public AbstractRDDNodeVisitor(
      @NonNull OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    super(context);
    this.datasetFactory = datasetFactory;
  }

  protected List<D> findInputDatasets(List<RDD<?>> fileRdds, StructType schema) {
    return PlanUtils.findRDDPaths(fileRdds).stream()
        .map(
            p -> {
              // TODO- refactor this to return a single partitioned dataset based on static
              // static partitions in the relation
              return datasetFactory.getDataset(p.toUri(), schema);
            })
        .collect(Collectors.toList());
  }
}
