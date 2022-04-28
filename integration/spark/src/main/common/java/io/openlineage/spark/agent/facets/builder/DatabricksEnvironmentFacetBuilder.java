package io.openlineage.spark.agent.facets.builder;

import com.databricks.backend.daemon.dbutils.MountInfo;
import com.databricks.dbutils_v1.DbfsUtils;
import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.agent.models.DatabricksMountpoint;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.collection.JavaConversions;

/**
 * {@link CustomFacetBuilder} that generates a {@link EnvironmentFacet} when using OpenLineage on
 * Databricks.
 */
@Slf4j
public class DatabricksEnvironmentFacetBuilder
    extends CustomFacetBuilder<Object, EnvironmentFacet> {
  private HashMap<String, Object> dbProperties;
  private final OpenLineageContext openLineageContext;
  private Class dbutilsClass;
  private DbfsUtils dbutils;

  public DatabricksEnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return (x instanceof SparkListenerSQLExecutionEnd
        || x instanceof SparkListenerSQLExecutionStart
        || x instanceof SparkListenerJobStart);
  }

  public static boolean isDatabricksRuntime() {
    return System.getenv().containsKey("DATABRICKS_RUNTIME_VERSION");
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    if (event instanceof SparkListenerJobStart) {
      SparkListenerJobStart jobStart = (SparkListenerJobStart) event;
      log.info(String.format("WILLJ: EnvFacet JobId:%s", Integer.toString(jobStart.jobId())));
      consumer.accept(
          "environment-properties",
          new EnvironmentFacet(getDatabricksJobStartProperties(jobStart)));
    } else if (event instanceof SparkListenerSQLExecutionStart) {
      SparkListenerSQLExecutionStart executionStart = (SparkListenerSQLExecutionStart) event;
      consumer.accept(
          "environment-properties",
          new EnvironmentFacet(getDatabricksSqlStartProperties(executionStart)));
    } else if (event instanceof SparkListenerSQLExecutionEnd) {
      SparkListenerSQLExecutionEnd executionEnd = (SparkListenerSQLExecutionEnd) event;
      consumer.accept(
          "environment-properties",
          new EnvironmentFacet(getDatabricksSqlEndProperties(executionEnd)));
    }
  }

  private HashMap<String, Object> getDatabricksJobStartProperties(SparkListenerJobStart jobStart) {
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
            "spark.databricks.clusterUsageTags.azureSubscriptionId",
            "spark.sql.execution.parent");
    dbPropertiesKeys.stream()
        .forEach(
            (p) -> {
              dbProperties.put(p, jobStart.properties().getProperty(p));
            });
    dbProperties.put("willjJobId", Integer.toString(jobStart.jobId()));
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

  private HashMap<String, Object> getDatabricksSqlStartProperties(
      SparkListenerSQLExecutionStart event) {
    dbProperties = new HashMap<>();
    dbProperties.put("execution-id", String.valueOf(event.executionId()));
    return dbProperties;
  }

  private HashMap<String, Object> getDatabricksSqlEndProperties(
      SparkListenerSQLExecutionEnd event) {
    dbProperties = new HashMap<>();
    dbProperties.put("execution-id", String.valueOf(event.executionId()));
    return dbProperties;
  }
}
