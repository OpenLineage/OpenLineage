/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.CreateClusterResponse;
import com.databricks.sdk.service.compute.ListClustersRequest;
import com.databricks.sdk.service.files.CreateResponse;
import com.databricks.sdk.service.files.Delete;
import com.databricks.sdk.service.jobs.Source;
import com.databricks.sdk.service.jobs.SparkPythonTask;
import com.databricks.sdk.service.jobs.SubmitRun;
import com.databricks.sdk.service.jobs.SubmitTask;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.databricks.ClusterLogConf;
import io.openlineage.spark.agent.databricks.CreateCluster;
import io.openlineage.spark.agent.databricks.InitScript;
import io.openlineage.spark.agent.databricks.WorkspaceDestination;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class DatabricksUtils {

  public static final String CLUSTER_NAME = "openlineage-test-cluster";
  public static final Map<String, String> PLATFORM_VERSIONS =
      Stream.of(new AbstractMap.SimpleEntry<>("3.4.0", "13.0.x-scala2.12"))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  public static final String NODE_TYPE = "Standard_DS3_v2";
  public static final String DBFS_EVENTS_FILE = "dbfs:/databricks/openlineage/events.log";
  public static final String INIT_SCRIPT_FILE = "/Shared/open-lineage-init-script.sh";
  public static final String DBFS_CLUSTER_LOGS = "dbfs:/databricks/openlineage/cluster-logs";
  private static final String SPARK_VERSION = "spark.version";

  @SneakyThrows
  static String init(WorkspaceClient workspace) {
    uploadOpenlineageJar(workspace);

    // clear cluster logs
    Delete deleteClusterLogs = new Delete();
    deleteClusterLogs.setPath(DBFS_CLUSTER_LOGS);
    deleteClusterLogs.setRecursive(true);
    workspace.dbfs().delete(deleteClusterLogs);

    // check if cluster is available
    String clusterId;
    Iterable<ClusterDetails> clusterDetails = workspace.clusters().list(new ListClustersRequest());
    if (clusterDetails != null) {
      clusterId =
          StreamSupport.stream(clusterDetails.spliterator(), false)
              .filter(cl -> cl.getClusterName().equals(getClusterName()))
              .map(cl -> cl.getClusterId())
              .findAny()
              .orElseGet(() -> createCluster(workspace));
    } else {
      clusterId = createCluster(workspace);
    }

    log.info("Ensuring cluster is running");
    workspace.clusters().ensureClusterIsRunning(clusterId);

    return clusterId;
  }

  @SneakyThrows
  static void shutdown(WorkspaceClient workspace, String clusterId) {
    // remove events file
    workspace.dbfs().delete(DBFS_EVENTS_FILE);

    // need to terminate cluster to have access to cluster logs
    workspace.clusters().delete(clusterId);
    workspace.clusters().waitGetClusterTerminated(clusterId);

    // wait for logs to be available
    Path clusterLogs = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/log4j-active.log");

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(3))
        .until(
            () -> {
              try {
                return workspace.dbfs().getStatus(clusterLogs.toString()) != null;
              } catch (Exception e) {
                return false;
              }
            });

    // fetch logs and move to local file
    FileWriter fileWriter = new FileWriter("./build/cluster-log4j.log");
    workspace.dbfs().readAllLines(clusterLogs, StandardCharsets.UTF_8).stream()
        .forEach(
            line -> {
              try {
                fileWriter.write(line + System.lineSeparator());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    workspace.dbfs().delete(clusterLogs.toAbsolutePath().toString());
  }

  @SneakyThrows
  static List<RunEvent> runScript(WorkspaceClient workspace, String clusterId, String scriptName) {
    // upload scripts
    String dbfsScriptPath = "dbfs:/databricks/openlineage/scripts/" + scriptName;
    String taskName = scriptName.replace(".py", "");

    workspace
        .dbfs()
        .write(
            Paths.get(dbfsScriptPath),
            readAllBytes(
                Paths.get(Resources.getResource("databricks_notebooks/" + scriptName).getPath())));

    SparkPythonTask task = new SparkPythonTask();
    task.setSource(Source.WORKSPACE);
    task.setPythonFile(dbfsScriptPath);

    SubmitTask runSubmitTaskSettings = new SubmitTask();
    runSubmitTaskSettings.setTaskKey(taskName);
    runSubmitTaskSettings.setExistingClusterId(clusterId);
    runSubmitTaskSettings.setSparkPythonTask(task);

    SubmitRun submitRun = new SubmitRun();
    submitRun.setRunName(taskName);
    submitRun.setTasks(Collections.singletonList(runSubmitTaskSettings));

    // trigger one time job
    workspace.jobs().submit(submitRun).get();

    return fetchEventsEmitted(workspace);
  }

  @SneakyThrows
  private static String createCluster(WorkspaceClient workspace) {
    CreateCluster createCluster =
        CreateCluster.builder()
            .cluster_name(getClusterName())
            .spark_version(getSparkPlatformVersion())
            .node_type_id(NODE_TYPE)
            .autotermination_minutes(10L)
            .num_workers(1L)
            .init_scripts(
                new InitScript[] {new InitScript(new WorkspaceDestination(INIT_SCRIPT_FILE))})
            .spark_conf(
                new HashMap<String, String>() {
                  {
                    put("spark.openlineage.transport.type", "file");
                    put("spark.openlineage.transport.location", "/tmp/events.log");
                    put(
                        "spark.extraListeners",
                        "io.openlineage.spark.agent.OpenLineageSparkListener");
                    put("spark.openlineage.version", "v1");
                  }
                })
            .cluster_log_conf(new ClusterLogConf(new WorkspaceDestination(DBFS_CLUSTER_LOGS)))
            .build();

    log.info("Creating cluster");
    CreateClusterResponse response =
        workspace
            .apiClient()
            .POST("/api/2.0/clusters/create", createCluster, CreateClusterResponse.class);

    return response.getClusterId();
  }

  @NotNull
  private static String getClusterName() {
    return CLUSTER_NAME + "_" + getSparkPlatformVersion();
  }

  private static String getSparkPlatformVersion() {
    if (!PLATFORM_VERSIONS.containsKey(System.getProperty(SPARK_VERSION))) {
      log.error("Unsupported spark_version for databricks test");
    }

    return PLATFORM_VERSIONS.get(System.getProperty(SPARK_VERSION));
  }

  @SneakyThrows
  private static void uploadOpenlineageJar(WorkspaceClient workspace) {
    Path jarFile =
        Files.list(Paths.get("../build/libs/"))
            .filter(p -> p.getFileName().toString().startsWith("openlineage-spark-"))
            .filter(p -> p.getFileName().toString().endsWith("jar"))
            .findAny()
            .orElseThrow(() -> new RuntimeException("openlineage-spark jar not found"));

    // make sure dbfs:/databricks/openlineage/ exists
    try {
      workspace.dbfs().mkdirs("dbfs:/databricks");
    } catch (RuntimeException e) {
    }
    try {
      workspace.dbfs().mkdirs("dbfs:/databricks/openlineage/");
    } catch (RuntimeException e) {
    }

    // clear other jars in DBFS
    if (workspace.dbfs().list("dbfs:/databricks/openlineage/") != null) {
      StreamSupport.stream(
              workspace.dbfs().list("dbfs:/databricks/openlineage/").spliterator(), false)
          .filter(f -> f.getPath().contains("openlineage-spark"))
          .filter(f -> f.getPath().endsWith(".jar"))
          .forEach(f -> workspace.dbfs().delete(f.getPath()));
    }

    // upload to DBFS -> 12MB file upload need to go in chunks smaller than 1MB each
    CreateResponse createResponse =
        workspace.dbfs().create("dbfs:/databricks/openlineage/" + jarFile.getFileName());

    FileInputStream fis = new FileInputStream(jarFile.toString());
    OutputStream outputStream =
        workspace.dbfs().getOutputStream("dbfs:/databricks/openlineage/" + jarFile.getFileName());

    byte[] buf = new byte[500000]; // approx 0.5MB
    int len = -1;
    while ((len = fis.read(buf)) != -1) {
      outputStream.write(buf, 0, len);
      outputStream.flush();
    }
    outputStream.close();
  }

  @SneakyThrows
  private static List<RunEvent> fetchEventsEmitted(WorkspaceClient workspace) {
    return workspace.dbfs().readAllLines(Paths.get(DBFS_EVENTS_FILE), StandardCharsets.UTF_8)
        .stream()
        .map(event -> OpenLineageClientUtils.runEventFromJson(event))
        .collect(Collectors.toList());
  }
}
