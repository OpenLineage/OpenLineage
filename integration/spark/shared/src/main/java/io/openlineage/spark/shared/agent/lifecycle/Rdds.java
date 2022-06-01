/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.lifecycle;

import io.openlineage.spark.shared.agent.util.ScalaConversionUtils;
import org.apache.spark.Dependency;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.ShuffledRowRDD;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;
import java.util.stream.Collectors;

public class Rdds {

  public static Set<RDD<?>> flattenRDDs(RDD<?> rdd) {
    Set<RDD<?>> rdds = new HashSet<>();
    rdds.add(rdd);
    if (rdd instanceof ShuffledRowRDD) {
      rdds.addAll(flattenRDDs(((ShuffledRowRDD) rdd).dependency().rdd()));
    }
    Collection<Dependency<?>> deps = JavaConversions.asJavaCollection(rdd.dependencies());
    for (Dependency<?> dep : deps) {
      rdds.addAll(flattenRDDs(dep.rdd()));
    }
    return rdds;
  }

  static String toString(SparkListenerJobStart jobStart) {
    StringBuilder sb = new StringBuilder();
    sb.append("start: ").append(jobStart.properties().toString()).append("\n");
    List<StageInfo> stageInfos = JavaConversions.seqAsJavaList(jobStart.stageInfos());
    for (StageInfo stageInfo : stageInfos) {
      sb.append("  ")
          .append("stageInfo: ")
          .append(stageInfo.stageId())
          .append(" ")
          .append(stageInfo.name())
          .append("\n");
      List<RDDInfo> rddInfos = JavaConversions.seqAsJavaList(stageInfo.rddInfos());
      for (RDDInfo rddInfo : rddInfos) {
        sb.append("    ").append("rddInfo: ").append(rddInfo).append("\n");
      }
    }
    return sb.toString();
  }

  public static List<RDD<?>> findFileLikeRdds(RDD<?> rdd) {
    List<RDD<?>> ret = new ArrayList<>();
    Stack<RDD<?>> deps = new Stack<>();
    deps.add(rdd);
    while (!deps.isEmpty()) {
      RDD<?> cur = deps.pop();
      Seq<Dependency<?>> dependencies = cur.getDependencies();
      deps.addAll(
          ScalaConversionUtils.fromSeq(dependencies).stream()
              .map(Dependency::rdd)
              .collect(Collectors.toList()));
      if (cur instanceof HadoopRDD) {
        ret.add(cur);
      } else if (cur instanceof FileScanRDD) {
        ret.add(cur);
      }
    }
    return ret;
  }
}
