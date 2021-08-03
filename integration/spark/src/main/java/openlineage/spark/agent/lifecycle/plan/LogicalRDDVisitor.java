package openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.Dependency;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that attempts to extract {@link Path}s from a {@link HadoopRDD}
 * wrapped in a {@link LogicalRDD}.The logic is mostly the same as the {@link
 * org.apache.spark.sql.execution.datasources.HadoopFsRelation}, but works with {@link RDD}s that
 * are converted to {@link org.apache.spark.sql.Dataset}s.
 */
public class LogicalRDDVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof LogicalRDD && !findHadoopRdds((LogicalRDD) x).isEmpty();
  }

  private List<HadoopRDD> findHadoopRdds(LogicalRDD rdd) {
    RDD root = rdd.rdd();
    List<HadoopRDD> ret = new ArrayList<>();
    Stack<RDD> deps = new Stack<>();
    deps.add(root);
    while (!deps.isEmpty()) {
      RDD cur = deps.pop();
      Seq<Dependency> dependencies = cur.getDependencies();
      deps.addAll(
          ScalaConversionUtils.fromSeq(dependencies).stream()
              .map(Dependency::rdd)
              .collect(Collectors.toList()));
      if (cur instanceof HadoopRDD) {
        ret.add((HadoopRDD) cur);
      }
    }
    return ret;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    LogicalRDD logicalRdd = (LogicalRDD) x;
    List<HadoopRDD> hadoopRdds = findHadoopRdds(logicalRdd);
    return hadoopRdds.stream()
        .flatMap(
            rdd -> {
              Path[] inputPaths = FileInputFormat.getInputPaths(rdd.getJobConf());
              Configuration hadoopConf = rdd.getConf();
              return Arrays.stream(inputPaths).map(p -> PlanUtils.getDirectoryPath(p, hadoopConf));
            })
        .distinct()
        .map(
            p -> {
              // TODO- refactor this to return a single partitioned dataset based on static
              // static partitions in the relation
              return PlanUtils.getDataset(p.toUri(), logicalRdd.schema());
            })
        .collect(Collectors.toList());
  }
}
