/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.mixin.DbfsExt;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.ClusterLogConf;
import com.databricks.sdk.service.compute.CreateCluster;
import com.databricks.sdk.service.compute.CreateClusterResponse;
import com.databricks.sdk.service.compute.DataSecurityMode;
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
import com.google.common.collect.ImmutableMap;
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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksEnvironment implements AutoCloseable {

  public static final String CLUSTER_NAME = "openlineage-test-cluster";
  public static final Map<String, String> PLATFORM_VERSIONS_NAMES =
      ImmutableMap.of("3.4.2", "13.3.x-scala2.12", "3.5.2", "14.2.x-scala2.12");
  public static final Map<String, String> PLATFORM_VERSIONS =
      ImmutableMap.of("3.4.2", "13.3", "3.5.2", "14.2");
  public static final String NODE_TYPE = "Standard_DS3_v2";
  public static final String INIT_SCRIPT_FILE = "/Shared/open-lineage-init-script.sh";
  public static final String DBFS_CLUSTER_LOGS = "dbfs:/databricks/openlineage/cluster-logs";
  private static final String executionTimestamp =
      ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
  private final DatabricksEnvironmentProperties properties;
  private final WorkspaceClient workspace;
  private final DbfsExt dbfs;
  private final String clusterId;
  @Getter private final String platformVersion;
  private final String dbfsEventsFile;
  private final Path clusterLogs;
  private final Path stdoutLogs;
  private final Path stdout;

  @Builder
  @Getter
  static class DatabricksEnvironmentProperties {
    private Workspace workspace;
    private Cluster cluster;
    private Development development;

    @Builder
    @Getter
    static class Workspace {
      private String host;
      private String token;
    }

    @Builder
    @Getter
    static class Cluster {
      private String sparkVersion;
    }

    @Builder
    @Getter
    static class Development {
      private String existingClusterId;
      private boolean preventClusterTermination;
      private String log4jLogsLocation;
      private boolean fetchLog4jLogs;
      private String stdoutLocation;
      private boolean fetchStdout;
      private String stderrLocation;
      private boolean fetchStderr;
      private String eventsFileLocation;
      private boolean fetchEvents;
    }
  }

  DatabricksEnvironment(DatabricksEnvironmentProperties properties) {
    log.info("Initializing Databricks environment");
    this.properties = properties;
    this.workspace =
        new WorkspaceClient(
            new DatabricksConfig()
                .setHost(properties.getWorkspace().getHost())
                .setToken(properties.getWorkspace().getToken()));
    this.dbfs = workspace.dbfs();

    uploadOpenLineageJar();

    // Create cluster or connect to an existing one
    String resolvedClusterId = properties.getDevelopment().getExistingClusterId();
    boolean attachingToExistingCluster = !"".equals(resolvedClusterId);
    if (attachingToExistingCluster) {
      log.info("Attaching to the existing cluster [{}]", resolvedClusterId);
      /*
         OpenLineage jars are copied from DBFS to the cluster during the initialization script execution.
         To ensure the updated jar is used, the cluster must be restarted after updating the jar.
         Without a restart, the initialization script won't run again, and the old jar will still be used.
      */
      log.warn(
          "⚠️ The cluster must be restarted to apply changes if the OpenLineage jar has been updated. ⚠️");
      this.clusterId = resolvedClusterId;
    } else {
      this.clusterId = prepareNewCluster();
    }

    this.platformVersion =
        PLATFORM_VERSIONS.get(properties.cluster.getSparkVersion()).replace(".", "_");
    this.dbfsEventsFile = "dbfs:/databricks/openlineage/events_" + this.platformVersion + ".log";
    this.clusterLogs = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/log4j-active.log");
    this.stdoutLogs = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/stdout");
    this.stdout = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/stderr");
  }

  @SneakyThrows
  private String prepareNewCluster() {
    uploadInitializationScript();

    // We may reuse the cluster name where there are existing old logs. This can happen if the
    // tests failed. Here we make sure the logs are clean.
    Delete deleteClusterLogs = new Delete();
    deleteClusterLogs.setPath(DBFS_CLUSTER_LOGS);
    deleteClusterLogs.setRecursive(true);
    dbfs.delete(deleteClusterLogs);

    log.info("Creating a new Databricks cluster.");
    String sparkPlatformVersion = getSparkPlatformVersion();
    String clusterName = CLUSTER_NAME + "_" + sparkPlatformVersion;
    ensureClusterDoesntExist(clusterName);
    Wait<ClusterDetails, CreateClusterResponse> cluster =
        createCluster(clusterName, sparkPlatformVersion);

    String clusterId = cluster.getResponse().getClusterId();
    log.info("Ensuring the new cluster [{}] with ID [{}] is running...", clusterName, clusterId);
    cluster.get(Duration.ofMinutes(10));
    return clusterId;
  }

  private void ensureClusterDoesntExist(String clusterName) {
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
  }

  @SneakyThrows
  @Override
  public void close() {
    boolean existingClusterUsed = !"".equals(properties.getDevelopment().getExistingClusterId());

    log.info("Deleting events file [{}]", dbfsEventsFile);
    deleteEventsFile();

    if (!(properties.getDevelopment().isPreventClusterTermination() || existingClusterUsed)) {
      log.info("Terminating cluster [{}].", clusterId);
      workspace.clusters().delete(clusterId);
      workspace.clusters().waitGetClusterTerminated(clusterId);
    }

    log.info("Deleting cluster logs from [{}].", clusterLogs);
    dbfs.delete(clusterLogs.toAbsolutePath().toString());
    dbfs.delete(stdoutLogs.toAbsolutePath().toString());
    dbfs.delete(stdout.toAbsolutePath().toString());
  }

  public void deleteEventsFile() {
    dbfs.delete(dbfsEventsFile);
  }

  /** Fetches driver's stdout, stderr and log4j logs files */
  @SneakyThrows
  public void fetchLogs() {
    DatabricksEnvironmentProperties.Development development = properties.getDevelopment();
    if (development.isFetchLog4jLogs()) {
      log.info("Fetching log4j logs");
      fetchLogs(
          clusterLogs,
          development.getLog4jLogsLocation() + "/" + executionTimestamp + "-cluster-log4j.log");
    } else {
      log.info("Skipping fetching log4j logs.");
    }

    if (development.isFetchStdout()) {
      log.info("Fetching stdout logs");
      fetchLogs(stdoutLogs, development.getStdoutLocation() + "/" + executionTimestamp + "_stdout");
    } else {
      log.info("Skipping fetching stdout logs.");
    }

    if (development.isFetchStderr()) {
      log.info("Fetching stderr logs");
      fetchLogs(stdout, development.getStderrLocation() + "/" + executionTimestamp + "_stderr");
    } else {
      log.info("Skipping fetching stderr logs.");
    }
  }

  private void fetchLogs(Path databricksLocation, String logsLocation) throws IOException {
    log.info("Waiting for the logs to be available under [{}]...", databricksLocation);
    await()
        .atMost(Duration.ofSeconds(300))
        .pollInterval(Duration.ofSeconds(3))
        .until(
            () -> {
              try {
                return dbfs.getStatus(databricksLocation.toString()) != null;
              } catch (Exception e) {
                return false;
              }
            });
    log.info("Fetching logs to [{}].", logsLocation);
    writeLinesToFile(logsLocation, dbfs.readAllLines(databricksLocation, StandardCharsets.UTF_8));
    log.info("Logs fetched.");
  }

  @SneakyThrows
  public List<RunEvent> runScript(String scriptName) {
    // upload scripts
    String dbfsScriptPath = "dbfs:/databricks/openlineage/scripts/" + scriptName;
    log.info("Uploading script [{}] to [{}]", scriptName, dbfsScriptPath);
    String taskName = scriptName.replace(".py", "");

    dbfs.write(
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

    return fetchEventsEmitted(scriptName);
  }

  @SneakyThrows
  private Wait<ClusterDetails, CreateClusterResponse> createCluster(
      String clusterName, String sparkPlatformVersion) {
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
            .setDataSecurityMode(DataSecurityMode.SINGLE_USER)
            .setSparkVersion(sparkPlatformVersion)
            .setNodeTypeId(NODE_TYPE)
            .setAutoterminationMinutes(10L)
            .setNumWorkers(1L)
            .setInitScripts(
                ImmutableList.of(
                    new InitScriptInfo()
                        .setWorkspace(new WorkspaceStorageInfo().setDestination(INIT_SCRIPT_FILE))))
            .setSparkConf(sparkConf)
            .setDataSecurityMode(DataSecurityMode.SINGLE_USER)
            .setClusterLogConf(
                new ClusterLogConf()
                    .setDbfs(new DbfsStorageInfo().setDestination(DBFS_CLUSTER_LOGS)));

    log.info("Creating cluster [{}]...", createCluster);
    workspace.config().setHttpTimeoutSeconds(600); // 10 minutes, otherwise it's rather setup issue
    return workspace.clusters().create(createCluster);
  }

  private String getSparkPlatformVersion() {
    String sparkVersion = properties.getCluster().getSparkVersion();
    if (!PLATFORM_VERSIONS_NAMES.containsKey(sparkVersion)) {
      log.error(
          "Unsupported [spark.version] for Databricks test: [{}]. Supported versions are {}",
          sparkVersion,
          PLATFORM_VERSIONS_NAMES.keySet());
      throw new IllegalStateException("Unsupported [spark.version] for Databricks");
    }

    String platformVersion = PLATFORM_VERSIONS_NAMES.get(sparkVersion);
    log.info("Databricks version: [{}].", platformVersion);
    return platformVersion;
  }

  /**
   * Copies the jar to the DBFS location from where it is copied to the driver host by the
   * initialization script. The copying happens only on the cluster initialization, so you have to
   * restart the cluster if you change the jar and want to use it.
   */
  @SneakyThrows
  private void uploadOpenLineageJar() {
    Path jarFile =
        Files.list(Paths.get("../build/libs/"))
            .filter(p -> p.getFileName().toString().startsWith("openlineage-spark_"))
            .filter(p -> p.getFileName().toString().endsWith("-SNAPSHOT.jar"))
            .findAny()
            .orElseThrow(() -> new RuntimeException("openlineage-spark jar not found"));

    // make sure dbfs:/databricks/openlineage/ exists
    try {
      dbfs.mkdirs("dbfs:/databricks");
    } catch (RuntimeException e) {
    }
    try {
      dbfs.mkdirs("dbfs:/databricks/openlineage/");
    } catch (RuntimeException e) {
    }

    // clear other jars in DBFS
    if (dbfs.list("dbfs:/databricks/openlineage/") != null) {
      StreamSupport.stream(dbfs.list("dbfs:/databricks/openlineage/").spliterator(), false)
          .filter(f -> f.getPath().contains("openlineage-spark"))
          .filter(f -> f.getPath().endsWith(".jar"))
          .forEach(f -> dbfs.delete(f.getPath()));
    }

    String destination = "dbfs:/databricks/openlineage/" + jarFile.getFileName();
    uploadFileToDbfs(jarFile, destination);
    log.info("OpenLineage jar has been uploaded to [{}]", destination);
  }

  /**
   * Uploads the cluster initialization script to DBFS.
   *
   * <p>The script is used by the clusters to copy OpenLineage jar to the location where it can be
   * loaded by the driver.
   */
  private void uploadInitializationScript() throws IOException {
    String string =
        Resources.toString(
            Paths.get("../databricks/open-lineage-init-script.sh").toUri().toURL(),
            StandardCharsets.UTF_8);
    String encodedString = Base64.getEncoder().encodeToString(string.getBytes());
    this.workspace
        .workspace()
        .importContent(
            new Import()
                .setPath(INIT_SCRIPT_FILE)
                .setContent(encodedString)
                .setFormat(ImportFormat.AUTO)
                .setOverwrite(true));
  }

  @SneakyThrows
  private void uploadFileToDbfs(Path jarFile, String toLocation) {
    FileInputStream fis = new FileInputStream(jarFile.toString());
    OutputStream outputStream = dbfs.getOutputStream(toLocation);

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
  private List<RunEvent> fetchEventsEmitted(String scriptName) {
    Path path = Paths.get(dbfsEventsFile);
    log.info("Fetching events from [{}]...", path);

    List<String> eventsLines = dbfs.readAllLines(path, StandardCharsets.UTF_8);
    log.info("There are [{}] events.", eventsLines.size());

    if (properties.getDevelopment().isFetchEvents()) {
      saveEventsLocally(scriptName, eventsLines);
    } else {
      log.info("Skipping fetching events logs.");
    }

    return eventsLines.stream()
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }

  /** Downloads the events locally for troubleshooting purposes */
  private void saveEventsLocally(String scriptName, List<String> lines) throws IOException {
    // The source file path is reused and deleted before every test. As long as the tests are not
    // executed concurrently, it should contain the events from the current test.
    String eventsLocation =
        properties.getDevelopment().getEventsFileLocation()
            + "/"
            + executionTimestamp
            + "-"
            + scriptName
            + "-events.ndjson";
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
