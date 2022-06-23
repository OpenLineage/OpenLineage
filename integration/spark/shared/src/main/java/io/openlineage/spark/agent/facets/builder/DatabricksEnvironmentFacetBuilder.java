package io.openlineage.spark.agent.facets.builder;

import com.databricks.backend.daemon.dbutils.MountInfo;
import com.databricks.dbutils_v1.DbfsUtils;
import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.agent.models.DatabricksMountpoint;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerJobStart;
import scala.collection.JavaConversions;

/**
 * {@link CustomFacetBuilder} that generates a {@link EnvironmentFacet} when using OpenLineage on
 * Databricks.
 */
@Slf4j
public class DatabricksEnvironmentFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobStart, EnvironmentFacet> {
  private Map<String, Object> dbProperties;
  private Class dbutilsClass;
  private DbfsUtils dbutils;

  public static boolean isDatabricksRuntime() {
    return System.getenv().containsKey("DATABRICKS_RUNTIME_VERSION");
  }

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    consumer.accept(
        "environment-properties",
        new EnvironmentFacet(getDatabricksEnvironmentalAttributes(event)));
  }

  private Map<String, Object> getDatabricksEnvironmentalAttributes(SparkListenerJobStart jobStart) {
    dbProperties = new HashMap<>();
    // These are useful properties to extract if they are available

    List<String> dbPropertiesKeys =
        Arrays.asList(
            "orgId",
            "spark.databricks.clusterUsageTags.clusterOwnerOrgId",
            "spark.databricks.notebook.path",
            "spark.databricks.job.type",
            "spark.databricks.job.id",
            "spark.databricks.job.runId",
            "user",
            "userId",
            "spark.databricks.clusterUsageTags.clusterName",
            "spark.databricks.clusterUsageTags.azureSubscriptionId");
    dbPropertiesKeys.stream()
        .forEach(
            (p) -> {
              dbProperties.put(p, jobStart.properties().getProperty(p));
            });

    /**
     * Azure Databricks makes available a dbutils mount point to list aliased paths to cloud
     * storage. However, that dbutils object is not available inside a spark listener. We must
     * access it via reflection.
     */
    try {
      dbutilsClass = Class.forName("com.databricks.dbutils_v1.impl.DbfsUtilsImpl");
      dbutils = (DbfsUtils) dbutilsClass.getDeclaredConstructor().newInstance();
      dbProperties.put("mountPoints", getDatabricksMountpoints(dbutils));
    } catch (Exception e) {
      log.warn("Failed to load dbutils in OpenLineageListener");
      dbProperties.put("mountPoints", new ArrayList<DatabricksMountpoint>());
    }

    return dbProperties;
  }

  private static List<DatabricksMountpoint> getDatabricksMountpoints(DbfsUtils dbutils) {
    List<DatabricksMountpoint> mountpoints = new ArrayList<>();
    List<MountInfo> mountsList = JavaConversions.seqAsJavaList(dbutils.mounts());
    for (MountInfo mount : mountsList) {
      mountpoints.add(new DatabricksMountpoint(mount.mountPoint(), mount.source()));
    }
    return mountpoints;
  }
}
