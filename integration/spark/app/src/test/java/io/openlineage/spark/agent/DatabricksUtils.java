/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.ClusterLogConf;
import com.databricks.sdk.service.compute.CreateCluster;
import com.databricks.sdk.service.compute.CreateClusterResponse;
import com.databricks.sdk.service.compute.DbfsStorageInfo;
import com.databricks.sdk.service.compute.InitScriptInfo;
import com.databricks.sdk.service.compute.ListClustersRequest;
import com.databricks.sdk.service.compute.WorkspaceStorageInfo;
import com.databricks.sdk.service.files.Delete;
import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.Source;
import com.databricks.sdk.service.jobs.SparkPythonTask;
import com.databricks.sdk.service.jobs.SubmitRun;
import com.databricks.sdk.service.jobs.SubmitRunResponse;
import com.databricks.sdk.service.jobs.SubmitTask;
import com.databricks.sdk.service.workspace.Import;
import com.databricks.sdk.service.workspace.ImportFormat;
import com.databricks.sdk.support.Wait;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
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
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksUtils {

  public static final String CLUSTER_NAME = "openlineage-test-cluster";
  public static final Map<String, String> PLATFORM_VERSIONS_NAMES =
      Stream.of(
              new AbstractMap.SimpleEntry<>("3.4.2", "13.3.x-scala2.12"),
              new AbstractMap.SimpleEntry<>("3.5.2", "14.2.x-scala2.12"))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  public static final Map<String, String> PLATFORM_VERSIONS =
      Stream.of(
              new AbstractMap.SimpleEntry<>("3.4.2", "13.3"),
              new AbstractMap.SimpleEntry<>("3.5.2", "14.2"))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  public static final String NODE_TYPE = "Standard_DS3_v2";
  public static final String INIT_SCRIPT_FILE = "/Shared/open-lineage-init-script.sh";
  public static final String DBFS_CLUSTER_LOGS = "dbfs:/databricks/openlineage/cluster-logs";
  public static final String DBFS_EVENTS_FILE =
      "dbfs:/databricks/openlineage/events_" + platformVersion() + ".log";

  public static String platformVersion() {
    return PLATFORM_VERSIONS
        .get(DatabricksDynamicParameter.SparkVersion.resolve())
        .replace(".", "_");
  }

  @SneakyThrows
  static String init(WorkspaceClient workspace) {
    String resolvedClusterId = DatabricksDynamicParameter.ClusterId.resolve();
    boolean attachingToExistingCluster = !"".equals(resolvedClusterId);

    uploadOpenLineageJar(workspace);
    uploadInitializationScript(workspace);

    if (attachingToExistingCluster) {
      log.info("Attaching to the existing cluster [{}]", resolvedClusterId);
      /*
         OpenLineage jars are copied from DBFS to the cluster during the initialization script execution.
         To ensure the updated jar is used, the cluster must be restarted after updating the jar.
         Without a restart, the initialization script won't run again, and the old jar will still be used.
      */
      log.warn(
          "⚠️ The cluster must be restarted to apply changes if the OpenLineage jar has been updated. ⚠️");
      return resolvedClusterId;
    } else {
      // We may reuse the cluster name where there are existing old logs. This can happen if the
      // tests failed. Here we make sure the logs are clean.
      Delete deleteClusterLogs = new Delete();
      deleteClusterLogs.setPath(DBFS_CLUSTER_LOGS);
      deleteClusterLogs.setRecursive(true);
      workspace.dbfs().delete(deleteClusterLogs);

      log.info("Creating a new Databricks cluster.");
      String sparkPlatformVersion = getSparkPlatformVersion();
      String clusterName = CLUSTER_NAME + "_" + getSparkPlatformVersion();
      log.debug("Ensuring the cluster with name [{}] doesn't exist.", clusterName);
      for (ClusterDetails clusterDetail : workspace.clusters().list(new ListClustersRequest())) {
        if (clusterDetail.getClusterName().equals(clusterName)) {
          log.info(
              "Deleting a cluster [{}] with ID [{}].",
              clusterDetail.getClusterName(),
              clusterDetail.getClusterId());
          workspace.clusters().permanentDelete(clusterDetail.getClusterId());
        }
      }
      Wait<ClusterDetails, CreateClusterResponse> cluster =
          createCluster(workspace, clusterName, sparkPlatformVersion);

      String clusterId = cluster.getResponse().getClusterId();
      log.info("Ensuring the new cluster [{}] with ID [{}] is running...", clusterName, clusterId);
      cluster.get(Duration.ofMinutes(10));
      return clusterId;
    }
  }

  @SneakyThrows
  static void shutdown(
      WorkspaceClient workspace,
      String clusterId,
      boolean preventClusterTermination,
      boolean existingClusterUsed,
      String executionTimestamp) {
    // remove events file
    workspace.dbfs().delete(DBFS_EVENTS_FILE);

    if (!(preventClusterTermination || existingClusterUsed)) {
      // need to terminate cluster to have access to cluster logs
      workspace.clusters().delete(clusterId);
      workspace.clusters().waitGetClusterTerminated(clusterId);
    }

    Path clusterLogs = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/log4j-active.log");
    log.info("Waiting for the cluster logs to be available on DBFS under [{}]...", clusterLogs);
    await()
        .atMost(Duration.ofSeconds(300))
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
    String logsLocation = "./build/" + executionTimestamp + "-cluster-log4j.log";
    log.info("Fetching cluster logs to [{}]", logsLocation);
    writeLinesToFile(
        logsLocation, workspace.dbfs().readAllLines(clusterLogs, StandardCharsets.UTF_8));
    log.info("Logs fetched.");

    workspace.dbfs().delete(clusterLogs.toAbsolutePath().toString());
  }

  @SneakyThrows
  static List<RunEvent> runScript(
      WorkspaceClient workspace, String clusterId, String scriptName, String executionTimestamp) {
    // upload scripts
    String dbfsScriptPath = "dbfs:/databricks/openlineage/scripts/" + scriptName;
    log.info("Uploading script [{}] to [{}]", scriptName, dbfsScriptPath);
    String taskName = scriptName.replace(".py", "");

    workspace
        .dbfs()
        .write(
            Paths.get(dbfsScriptPath),
            readAllBytes(
                Paths.get(Resources.getResource("databricks_notebooks/" + scriptName).getPath())));
    log.info("The script [{}] has been uploaded to [{}].", scriptName, dbfsScriptPath);

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
    log.info("Submitting PySpark task [{}]...", taskName);
    Wait<Run, SubmitRunResponse> submit = workspace.jobs().submit(submitRun);
    log.info("PySpark task [{}] submitted. Waiting for completion...", taskName);
    submit.get();
    log.info("PySpark task [{}] completed.", taskName);

    return fetchEventsEmitted(workspace, scriptName, executionTimestamp);
  }

  @SneakyThrows
  private static Wait<ClusterDetails, CreateClusterResponse> createCluster(
      WorkspaceClient workspace, String clusterName, String sparkPlatformVersion) {
    HashMap<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.openlineage.facets.debug.disabled", "false");
    sparkConf.put("spark.openlineage.transport.type", "file");
    // Each test case script should copy this file to dbfs:/databricks/openlineage/ at the end of
    // execution.
    sparkConf.put("spark.openlineage.transport.location", "/tmp/events.log");
    sparkConf.put("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener");
    CreateCluster createCluster =
        new CreateCluster()
            .setClusterName(clusterName)
            .setSparkVersion(sparkPlatformVersion)
            .setNodeTypeId(NODE_TYPE)
            .setAutoterminationMinutes(10L)
            .setNumWorkers(1L)
            .setInitScripts(
                ImmutableList.of(
                    new InitScriptInfo()
                        .setWorkspace(new WorkspaceStorageInfo().setDestination(INIT_SCRIPT_FILE))))
            .setSparkConf(sparkConf)
            .setClusterLogConf(
                new ClusterLogConf()
                    .setDbfs(new DbfsStorageInfo().setDestination(DBFS_CLUSTER_LOGS)));

    log.info("Creating cluster [{}]...", createCluster);
    workspace.config().setHttpTimeoutSeconds(600); // 10 minutes, otherwise it's rather setup issue
    return workspace.clusters().create(createCluster);
  }

  private static String getSparkPlatformVersion() {
    String sparkVersion = DatabricksDynamicParameter.SparkVersion.resolve();
    if (!PLATFORM_VERSIONS_NAMES.containsKey(sparkVersion)) {
      log.error("Unsupported [spark.version] for databricks test: [{}].", sparkVersion);
    }

    log.info("Databricks version: [{}].", PLATFORM_VERSIONS_NAMES.get(sparkVersion));
    return PLATFORM_VERSIONS_NAMES.get(sparkVersion);
  }

  /**
   * Copies the jar to the DBFS location from where it is copied to the driver host by the
   * initialization script. The copying happens only on the cluster initialization, so you have to
   * restart the cluster if you change the jar and want to use it.
   */
  @SneakyThrows
  private static void uploadOpenLineageJar(WorkspaceClient workspace) {
    Path jarFile =
        Files.list(Paths.get("../build/libs/"))
            .filter(p -> p.getFileName().toString().startsWith("openlineage-spark_"))
            .filter(p -> p.getFileName().toString().endsWith("-SNAPSHOT.jar"))
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

    String destination = "dbfs:/databricks/openlineage/" + jarFile.getFileName();
    uploadFileToDbfs(workspace, jarFile, destination);
    log.info("OpenLineage jar has been uploaded to [{}]", destination);
  }

  /**
   * Uploads the cluster initialization script to DBFS.
   *
   * <p>The script is used by the clusters to copy OpenLineage jar to the location where it can be
   * loaded by the driver.
   */
  private static void uploadInitializationScript(WorkspaceClient workspace) throws IOException {
    String string =
        Resources.toString(
            Paths.get("../databricks/open-lineage-init-script.sh").toUri().toURL(),
            StandardCharsets.UTF_8);
    String encodedString = Base64.getEncoder().encodeToString(string.getBytes());
    workspace
        .workspace()
        .importContent(
            new Import()
                .setPath(INIT_SCRIPT_FILE)
                .setContent(encodedString)
                .setFormat(ImportFormat.AUTO)
                .setOverwrite(true));
  }

  @SneakyThrows
  private static void uploadFileToDbfs(WorkspaceClient workspace, Path jarFile, String toLocation) {
    FileInputStream fis = new FileInputStream(jarFile.toString());
    OutputStream outputStream = workspace.dbfs().getOutputStream(toLocation);

    // upload to DBFS -> 12MB file upload need to go in chunks smaller than 1MB each
    byte[] buf = new byte[500000]; // approx 0.5MB
    int len = fis.read(buf);
    while (len != -1) {
      outputStream.write(buf, 0, len);
      outputStream.flush();
      len = fis.read(buf);
    }
    outputStream.close();
  }

  @SneakyThrows
  private static List<RunEvent> fetchEventsEmitted(
      WorkspaceClient workspace, String scriptName, String executionTimestamp) {
    Path path = Paths.get(DBFS_EVENTS_FILE);
    log.info("Fetching events from [{}]...", path);

    List<String> eventsLines = workspace.dbfs().readAllLines(path, StandardCharsets.UTF_8);
    log.info("There are [{}] events.", eventsLines.size());

    saveEventsLocally(scriptName, executionTimestamp, eventsLines);

    return eventsLines.stream()
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }

  /** Downloads the events locally for troubleshooting purposes */
  private static void saveEventsLocally(
      String scriptName, String executionTimestamp, List<String> lines) throws IOException {
    // The source file path is reused and deleted before every test. As long as the tests are not
    // executed concurrently, it should contain the events from the current test.
    String eventsLocation = "./build/" + executionTimestamp + "-" + scriptName + "-events.ndjson";
    log.info("Fetching events to [{}]", eventsLocation);
    writeLinesToFile(eventsLocation, lines);
    log.info("Events fetched.");
  }

  private static void writeLinesToFile(String eventsLocation, List<String> lines)
      throws IOException {
    try (FileWriter fileWriter = new FileWriter(eventsLocation)) {
      lines.forEach(
          line -> {
            try {
              fileWriter.write(line + System.lineSeparator());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }
}
