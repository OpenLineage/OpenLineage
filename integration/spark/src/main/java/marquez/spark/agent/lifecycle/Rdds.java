package marquez.spark.agent.lifecycle;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.Dependency;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConversions;

public class Rdds {

  static Set<RDD<?>> flattenRDDs(RDD<?> rdd) {
    Set<RDD<?>> rdds = new HashSet<>();
    rdds.add(rdd);
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
}
