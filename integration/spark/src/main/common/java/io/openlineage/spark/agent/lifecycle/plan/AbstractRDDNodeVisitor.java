package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.types.StructType;

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
    return fileRdds.stream()
        .flatMap(
            rdd -> {
              if (rdd instanceof HadoopRDD) {
                HadoopRDD hadoopRDD = (HadoopRDD) rdd;
                Path[] inputPaths = FileInputFormat.getInputPaths(hadoopRDD.getJobConf());
                Configuration hadoopConf = hadoopRDD.getConf();
                return Arrays.stream(inputPaths)
                    .map(p -> PlanUtils.getDirectoryPath(p, hadoopConf));
              } else if (rdd instanceof FileScanRDD) {
                FileScanRDD fileScanRDD = (FileScanRDD) rdd;
                return ScalaConversionUtils.fromSeq(fileScanRDD.filePartitions()).stream()
                    .flatMap(fp -> Arrays.stream(fp.files()))
                    .map(f -> new Path(f.filePath()).getParent())
                    .filter(Objects::nonNull);
              } else {
                return Stream.empty();
              }
            })
        .distinct()
        .map(
            p -> {
              // TODO- refactor this to return a single partitioned dataset based on static
              // static partitions in the relation
              return datasetFactory.getDataset(p.toUri(), schema);
            })
        .collect(Collectors.toList());
  }
}
