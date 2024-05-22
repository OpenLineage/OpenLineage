/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.Job;
import com.databricks.sdk.service.jobs.ListJobsRequest;
import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.agent.models.DatabricksMountpoint;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

/**
 * {@link CustomFacetBuilder} that generates a {@link EnvironmentFacet} when using OpenLineage on
 * Databricks.
 */
public class DatabricksEnvironmentFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobStart, EnvironmentFacet> {
  private Map<String, Object> dbProperties;

  private static final Logger log =
      LoggerFactory.getLogger(DatabricksEnvironmentFacetBuilder.class);

  public static boolean isDatabricksRuntime() {
    return System.getenv().containsKey("DATABRICKS_RUNTIME_VERSION");
  }

  public DatabricksEnvironmentFacetBuilder() {}

  public DatabricksEnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
    dbProperties = new HashMap<>();
    // extract some custom environment variables if needed
    openLineageContext
        .getCustomEnvironmentVariables()
        .forEach(envVar -> dbProperties.put(envVar, System.getenv().get(envVar)));
  }

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    consumer.accept(
        "environment-properties",
        new EnvironmentFacet(getDatabricksEnvironmentalAttributes(event)));
  }

  private Map<String, Object> getDatabricksEnvironmentalAttributes(SparkListenerJobStart jobStart) {
    if (dbProperties == null) {
      dbProperties = new HashMap<>();
    }

    // These are useful properties to extract if they are available
    //    List<String> dbPropertiesKeys =
    //        Arrays.asList(
    //            "orgId",
    //            "spark.databricks.clusterUsageTags.clusterOwnerOrgId",
    //            "spark.databricks.notebook.path",
    //            "spark.databricks.job.type",
    //            "spark.databricks.job.id",
    //            "spark.databricks.job.name",
    //            "spark.databricks.job.runId",
    //            "spark.databricks.task.runId",
    //            "spark.databricks.task.name",
    //            "user",
    //            "userId",
    //            "spark.databricks.clusterUsageTags.clusterName",
    //            "spark.databricks.clusterUsageTags.clusterAllTags",
    //            "spark.databricks.clusterUsageTags.azureSubscriptionId");

    for (Map.Entry<Object, Object> entry : jobStart.properties().entrySet()) {
      log.error("Property {} - {}", entry.getKey(), entry.getValue());
      dbProperties.put(entry.getKey().toString(), entry.getValue().toString());
    }

    Object jobId = dbProperties.get("spark.databricks.job.id");
    String url = (String) jobStart.properties().get("spark.databricks.workspaceUrl");
    String token = (String) jobStart.properties().get("spark.databricks.token");

    if (jobId == null) {
      log.error("JOB ID IS NULL?");
    } else {
      if (token == null || token.equals("[REDACTED]")) {
        log.error("Job ID {}", jobId);
        log.error("NO TOKEN spark.databricks.token - {}", token);
        token =
            (String)
                jobStart
                    .properties()
                    .get("spark.databricks.inherited.credentials.keys.spark.databricks.api.token");
        if (token == null || token.equals("*********(redacted)")) {
          log.error(
              "NO TOKEN spark.databricks.inherited.credentials.keys.spark.databricks.api.token {}",
              token);
        }
      }
      DatabricksConfig config = new DatabricksConfig().setToken(token).setHost("https://" + url);
      WorkspaceClient workspaceClient = new WorkspaceClient(config);
      long jobIdLong;
      try {
        jobIdLong = Long.parseLong((String) jobId);
      } catch (NumberFormatException e) {
        log.error("Failed to parse job ID: {}", jobId);
        return dbProperties;
      }
      try {
        Job job = workspaceClient.jobs().get(jobIdLong);
        dbProperties.put("job", job);

      } catch (DatabricksError error) {
        log.error("DBX error: {}", error.toString());
        log.error("Printing all the jobs");
        for (BaseJob job : workspaceClient.jobs().list(new ListJobsRequest().setLimit(10L))) {
          log.error("Has job {} - {}", job.getJobId(), job);
        }
      }
    }

    /**
     * Azure Databricks makes available a dbutils mount point to list aliased paths to cloud
     * storage. However, that dbutils object is not available inside a spark listener. We must
     * access it via reflection.
     */
    dbProperties.put("mountPoints", getDatabricksMountpoints());

    return dbProperties;
  }

  // Starting in Databricks Runtime 11, there is a new constructor for DbFsUtils
  // If running on an older version, the constructor has no parameters.
  // If running on DBR 11 or above, you need to specify whether you allow mount operations (true or
  // false)
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private static List<DatabricksMountpoint> getDatabricksMountpoints() {
    Class dbutilsClass;
    try {
      dbutilsClass = Class.forName("com.databricks.dbutils_v1.impl.DbfsUtilsImpl");
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      log.warn("Class com.databricks.dbutils_v1.impl.DbfsUtilsImpl not found", e);
      return Collections.emptyList();
    }
    Constructor[] dbutilsConstructors = dbutilsClass.getDeclaredConstructors();
    if (dbutilsConstructors.length == 0) {
      log.warn(
          "Failed to load dbutils in OpenLineageListener as there were no declared constructors");
      return Collections.emptyList();
    }
    Constructor firstConstructor = dbutilsConstructors[0];
    Parameter[] constructorParams = firstConstructor.getParameters();
    Object dbfsUtils; // com.databricks.dbutils_v1.impl.DBUtilsV1Impl
    if (constructorParams.length == 0) {
      log.debug("DbUtils constructor had no parameters");
      try {
        dbfsUtils = firstConstructor.newInstance();
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        log.warn("DbUtils method thrown {}", e);
        return Collections.emptyList();
      }
    } else if (constructorParams.length == 1
        && "allowMountOperations".equals(constructorParams[0].getName())) {
      log.debug("DbUtils constructor had one parameter named allowMountOperations");
      try {
        dbfsUtils = firstConstructor.newInstance(true);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        log.warn("DbUtils method thrown {}", e);
        return Collections.emptyList();
      }
    } else {
      log.warn(
          "dbutils had {} constructors and the first constructor had {} params",
          dbutilsConstructors.length,
          constructorParams.length);
      return Collections.emptyList();
    }

    List<DatabricksMountpoint> mountpoints = new ArrayList<>();

    // list of com.databricks.backend.daemon.dbutils.MountInfo
    List<Object> mountsList =
        ScalaConversionUtils.fromSeq(
            (Seq<Object>) ReflectionUtils.tryExecuteMethod(dbfsUtils, "mounts").get());

    for (Object mount : mountsList) {
      Optional<Object> mountPoint = ReflectionUtils.tryExecuteMethod(mount, "mountPoint");
      Optional<Object> source = ReflectionUtils.tryExecuteMethod(mount, "source");

      if (mountPoint.isPresent()
          && mountPoint.get() != null
          && source.isPresent()
          && source.get() != null) {
        mountpoints.add(
            new DatabricksMountpoint(mountPoint.get().toString(), source.get().toString()));
      } else {
        log.warn(
            "Couldn't extract mountPoint and source through reflection. "
                + "mountPoint = {}, source = {}",
            mountPoint,
            source);
      }
    }

    return mountpoints;
  }
}
