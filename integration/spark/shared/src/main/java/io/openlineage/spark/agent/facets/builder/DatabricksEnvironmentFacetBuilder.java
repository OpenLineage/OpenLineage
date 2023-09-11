/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import com.databricks.backend.daemon.dbutils.MountInfo;
import com.databricks.dbutils_v1.DbfsUtils;
import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.agent.models.DatabricksMountpoint;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  public DatabricksEnvironmentFacetBuilder() {}

  public DatabricksEnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
    dbProperties = new HashMap<>();
    // extract some custom environment variables if needed
    openLineageContext
        .getCustomEnvironmentVariables()
        .ifPresent(
            envVars ->
                envVars.forEach(envVar -> dbProperties.put(envVar, System.getenv().get(envVar))));
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
            "spark.databricks.clusterUsageTags.clusterAllTags",
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
      Optional<DbfsUtils> dbfsUtils = getDbfsUtils();
      if (!dbfsUtils.isPresent()) {
        dbProperties.put("mountPoints", new ArrayList<DatabricksMountpoint>());
      } else {
        dbProperties.put("mountPoints", getDatabricksMountpoints(dbfsUtils.get()));
      }

    } catch (Exception e) {
      log.warn("Failed to load dbutils in OpenLineageListener:", e);
      dbProperties.put("mountPoints", new ArrayList<DatabricksMountpoint>());
    }
    return dbProperties;
  }

  // Starting in Databricks Runtime 11, there is a new constructor for DbFsUtils
  // If running on an older version, the constructor has no parameters.
  // If running on DBR 11 or above, you need to specify whether you allow mount operations (true or
  // false)
  private static Optional<DbfsUtils> getDbfsUtils()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException,
          IllegalArgumentException, InvocationTargetException {
    Class dbutilsClass = Class.forName("com.databricks.dbutils_v1.impl.DbfsUtilsImpl");
    Constructor[] dbutilsConstructors = dbutilsClass.getDeclaredConstructors();
    if (dbutilsConstructors.length == 0) {
      log.warn(
          "Failed to load dbutils in OpenLineageListener as there were no declared constructors");
      return Optional.empty();
    }
    Constructor firstConstructor = dbutilsConstructors[0];
    Parameter[] constructorParams = firstConstructor.getParameters();
    if (constructorParams.length == 0) {
      log.debug("DbUtils constructor had no parameters");
      return Optional.of((DbfsUtils) firstConstructor.newInstance());
    } else if (constructorParams.length == 1
        && constructorParams[0].getName().equals("allowMountOperations")) {
      log.debug("DbUtils constructor had one parameter named allowMountOperations");
      return Optional.of((DbfsUtils) firstConstructor.newInstance(true));
    } else {
      log.warn(
          "dbutils had {} constructors and the first constructor had {} params",
          dbutilsConstructors.length,
          constructorParams.length);
      return Optional.empty();
    }
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
