/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import com.databricks.sdk.service.compute.SparkVersion;
import com.databricks.sdk.service.compute.State;
import com.databricks.sdk.service.compute.WorkspaceStorageInfo;
import com.databricks.sdk.service.files.FileInfo;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Prepares a Databricks cluster for the integration tests and runs the test scripts on it.
 *
 * <p>The cluster is chosen in this order:
 *
 * <ol>
 *   <li>the cluster given by {@code development.clusterId},
 *   <li>a new cluster, created when {@code development.createCluster} is {@code true} (needs
 *       cluster creation rights),
 *   <li>otherwise, the only cluster named {@value #TEST_CLUSTER_NAME_PREFIX}... whose Databricks
 *       runtime matches the Spark and Scala versions of the build. This is what CI uses: the
 *       clusters are pre-created and pinned, and the CI principal can only restart them.
 * </ol>
 */
@Slf4j
public class DatabricksEnvironment implements AutoCloseable {

  /** CI clusters are found by this name prefix and their Databricks runtime. */
  public static final String TEST_CLUSTER_NAME_PREFIX = "openlineage-test";

  /**
   * Clusters created by the tests use another prefix, so a leftover one is never mistaken for a CI
   * cluster, and creating one never deletes a CI cluster with the same name.
   */
  public static final String CREATED_CLUSTER_NAME_PREFIX = "openlineage-dev-cluster";

  /**
   * Databricks runtime (major or major.minor) for each supported "spark.version/scala version".
   * 16.4 LTS is the last runtime with Spark 3.5. 18 LTS runs Spark 4.1 and only Scala 2.13.
   */
  public static final Map<String, String> DATABRICKS_RUNTIMES =
      ImmutableMap.of(
          "3.5.6/2.12", "16.4",
          "3.5.6/2.13", "16.4",
          "4.1.3/2.13", "18");

  private static final Duration CLUSTER_START_TIMEOUT = Duration.ofMinutes(10);
  public static final String NODE_TYPE = "Standard_DS3_v2";
  public static final String INIT_SCRIPT_FILE = "/Shared/open-lineage-init-script.sh";
  public static final String DBFS_CLUSTER_LOGS = "dbfs:/databricks/openlineage/cluster-logs";

  /**
   * Each cluster loads the OpenLineage jar from its own directory, named after the cluster, so jobs
   * running in parallel for different Scala versions never share or delete each other's jar.
   */
  public static final String DBFS_CLUSTER_JARS = "dbfs:/databricks/openlineage/clusters/";

  private static final String INIT_SCRIPT_STAGE_DIR = "STAGE_DIR=\"/dbfs/databricks/openlineage\"";
  private static final String INIT_SCRIPT_CLUSTER_STAGE_DIR =
      "STAGE_DIR=\"/dbfs/databricks/openlineage/clusters/${DB_CLUSTER_NAME}\"";

  private static final String executionTimestamp =
      ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
  private final DatabricksEnvironmentProperties properties;
  private final WorkspaceClient workspace;
  private final DbfsExt dbfs;
  private final String runtime;
  private final String scalaVersion;
  private final String clusterId;
  private final boolean clusterCreated;

  /**
   * Suffix for table names, paths and the events file, e.g. {@code 16_4_scala2_13}. It is passed to
   * the scripts, so that jobs for the same runtime but different Scala versions don't collide.
   */
  @Getter private final String platformVersion;

  private final String dbfsEventsFile;
  private final Path clusterLogs;
  private final Path stdoutLogs;
  private final Path stdout;

  @SuppressWarnings("PMD.UnusedAssignment")
  @Getter
  private boolean initializationSuccessful = true;

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
      private String scalaVersion;
    }

    @Builder
    @Getter
    static class Development {
      private String existingClusterId;
      private boolean createCluster;
      private boolean preventClusterTermination;
      private boolean restartExistingCluster;
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

  @SneakyThrows
  DatabricksEnvironment(DatabricksEnvironmentProperties properties) {
    log.info("Initializing Databricks environment");
    this.properties = properties;
    this.scalaVersion = properties.getCluster().getScalaVersion();
    this.runtime = resolveRuntime(properties.getCluster().getSparkVersion(), scalaVersion);
    this.platformVersion = runtime.replace(".", "_") + "_scala" + scalaVersion.replace(".", "_");
    this.workspace =
        new WorkspaceClient(
            new DatabricksConfig()
                .setHost(properties.getWorkspace().getHost())
                .setToken(properties.getWorkspace().getToken()));
    this.dbfs = workspace.dbfs();

    DatabricksEnvironmentProperties.Development development = properties.getDevelopment();
    String existingClusterId = development.getExistingClusterId();
    if (existingClusterId != null && !existingClusterId.isEmpty()) {
      log.info("Attaching to the existing cluster [{}]", existingClusterId);
      this.clusterId = existingClusterId;
      this.clusterCreated = false;
      uploadOpenLineageJar(workspace.clusters().get(existingClusterId).getClusterName());
      if (development.isRestartExistingCluster()) {
        uploadInitializationScript();
        this.initializationSuccessful = restartExistingCluster(existingClusterId);
      } else {
        /*
           OpenLineage jars are copied from DBFS to the cluster during the initialization script execution.
           To ensure the updated jar is used, the cluster must be restarted after updating the jar.
           Without a restart, the initialization script won't run again, and the old jar will still be used.
        */
        log.warn(
            "⚠️ The cluster must be restarted to apply changes if the OpenLineage jar has been updated. ⚠️");
      }
    } else if (development.isCreateCluster()) {
      String sparkVersionKey = resolveSparkVersionKey();
      String clusterName = CREATED_CLUSTER_NAME_PREFIX + "_" + sparkVersionKey;
      uploadOpenLineageJar(clusterName);
      uploadInitializationScript();
      ClusterInitialization clusterInitialization = createNewCluster(clusterName, sparkVersionKey);
      this.clusterId = clusterInitialization.getClusterId();
      this.clusterCreated = true;
      this.initializationSuccessful = clusterInitialization.isSuccessful();
    } else {
      ClusterDetails cluster = findTestCluster();
      log.info(
          "Using the cluster [{}] with ID [{}] on runtime [{}]",
          cluster.getClusterName(),
          cluster.getClusterId(),
          cluster.getSparkVersion());
      this.clusterId = cluster.getClusterId();
      this.clusterCreated = false;
      uploadOpenLineageJar(cluster.getClusterName());
      uploadInitializationScript();
      // the init script copies the jar onto the cluster only when the cluster starts
      this.initializationSuccessful = restartExistingCluster(clusterId);
    }

    this.dbfsEventsFile = "dbfs:/databricks/openlineage/events_" + this.platformVersion + ".log";
    this.clusterLogs = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/log4j-active.log");
    this.stdoutLogs = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/stdout");
    this.stdout = Paths.get(DBFS_CLUSTER_LOGS + "/" + clusterId + "/driver/stderr");
  }

  private static String resolveRuntime(String sparkVersion, String scalaVersion) {
    String key = sparkVersion + "/" + scalaVersion;
    if (!DATABRICKS_RUNTIMES.containsKey(key)) {
      throw new IllegalStateException(
          String.format(
              "Unsupported Spark/Scala versions for the Databricks tests: [%s]. Supported: %s",
              key, DATABRICKS_RUNTIMES.keySet()));
    }
    String runtime = DATABRICKS_RUNTIMES.get(key);
    log.info("Databricks runtime: [{}], Scala [{}].", runtime, scalaVersion);
    return runtime;
  }

  /** True for a runtime key such as {@code 16.4.x-scala2.13} of the required runtime and Scala. */
  private boolean matchesRuntime(String sparkVersionKey) {
    return sparkVersionKey != null
        && sparkVersionKey.startsWith(runtime + ".")
        && sparkVersionKey.endsWith("-scala" + scalaVersion);
  }

  /**
   * Finds the only cluster named {@value #TEST_CLUSTER_NAME_PREFIX}... on the required runtime.
   * Only clusters the principal has a permission on are listed, and terminated clusters only if
   * they are pinned or terminated within the last 30 days.
   */
  private ClusterDetails findTestCluster() {
    List<ClusterDetails> clusters =
        StreamSupport.stream(
                workspace.clusters().list(new ListClustersRequest()).spliterator(), false)
            .collect(Collectors.toList());
    List<ClusterDetails> matching =
        clusters.stream()
            .filter(c -> c.getClusterName() != null)
            .filter(c -> c.getClusterName().startsWith(TEST_CLUSTER_NAME_PREFIX))
            .filter(c -> matchesRuntime(c.getSparkVersion()))
            .collect(Collectors.toList());
    if (matching.size() != 1) {
      throw new IllegalStateException(
          String.format(
              "Expected exactly one cluster named [%s...] on Databricks runtime [%s] with Scala [%s], "
                  + "found %d. Create and pin one, and give the CI principal CAN_RESTART on it. "
                  + "Visible clusters: %s",
              TEST_CLUSTER_NAME_PREFIX,
              runtime,
              scalaVersion,
              matching.size(),
              clusters.stream()
                  .map(
                      c ->
                          String.format(
                              "%s (%s, %s)",
                              c.getClusterName(), c.getClusterId(), c.getSparkVersion()))
                  .collect(Collectors.toList())));
    }
    return matching.get(0);
  }

  /** Picks the plain (no ML, GPU, Photon or ARM) runtime key, preferring an LTS one. */
  private String resolveSparkVersionKey() {
    String key =
        workspace.clusters().sparkVersions().getVersions().stream()
            .filter(v -> matchesRuntime(v.getKey()))
            .filter(v -> v.getKey().indexOf('-') == v.getKey().lastIndexOf('-'))
            .max(
                Comparator.comparing(
                        (SparkVersion v) -> v.getName() != null && v.getName().contains("LTS"))
                    .thenComparing(SparkVersion::getKey))
            .map(SparkVersion::getKey)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "No Databricks runtime [%s] with Scala [%s] in the workspace",
                            runtime, scalaVersion)));
    log.info("Databricks runtime key: [{}].", key);
    return key;
  }

  @SneakyThrows
  private ClusterInitialization createNewCluster(String clusterName, String sparkVersionKey) {
    // Cluster logs are stored per cluster ID, so a new cluster never sees old logs. Do not wipe
    // DBFS_CLUSTER_LOGS here: it is shared with the other matrix jobs running in parallel.
    log.info("Creating a new Databricks cluster.");
    ensureClusterDoesntExist(clusterName);
    Wait<ClusterDetails, CreateClusterResponse> cluster =
        createCluster(clusterName, sparkVersionKey);

    String newClusterId = cluster.getResponse().getClusterId();
    log.info("Ensuring the new cluster [{}] with ID [{}] is running...", clusterName, newClusterId);
    try {
      cluster.get(CLUSTER_START_TIMEOUT);
    } catch (TimeoutException e) {
      log.error("Cluster [{}] failed to start within the specified time.", clusterName);
      return new ClusterInitialization(newClusterId, false);
    }
    return new ClusterInitialization(newClusterId, true);
  }

  /**
   * Starts a terminated cluster or restarts a running one and waits until it is running, so the
   * init script copies the just-uploaded OpenLineage jar onto it.
   *
   * @return true if the cluster is running after the operation
   */
  private boolean restartExistingCluster(String existingClusterId) {
    try {
      State state = workspace.clusters().get(existingClusterId).getState();
      log.info("Existing cluster [{}] is in state [{}]", existingClusterId, state);
      if (state == State.TERMINATING) {
        workspace.clusters().waitGetClusterTerminated(existingClusterId);
        state = State.TERMINATED;
      }
      if (state == State.TERMINATED) {
        log.info("Starting the existing cluster [{}]...", existingClusterId);
        workspace.clusters().start(existingClusterId).get(CLUSTER_START_TIMEOUT);
      } else if (state == State.PENDING || state == State.RESTARTING || state == State.RESIZING) {
        // already coming up; it will run the init script with the uploaded jar only if it was
        // started after the upload, so restart it once it is running
        workspace.clusters().waitGetClusterRunning(existingClusterId);
        workspace.clusters().restart(existingClusterId).get(CLUSTER_START_TIMEOUT);
      } else {
        log.info("Restarting the existing cluster [{}]...", existingClusterId);
        workspace.clusters().restart(existingClusterId).get(CLUSTER_START_TIMEOUT);
      }
      return true;
    } catch (Exception e) {
      log.error("Failed to (re)start the existing cluster [{}]", existingClusterId, e);
      return false;
    }
  }

  private void ensureClusterDoesntExist(String clusterName) {
    log.debug("Ensuring the cluster with name [{}] doesn't exist.", clusterName);
    for (ClusterDetails clusterDetail : workspace.clusters().list(new ListClustersRequest())) {
      if (clusterName.equals(clusterDetail.getClusterName())) {
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
    log.info("Deleting events file [{}]", dbfsEventsFile);
    deleteEventsFile();

    // only clusters created by this run are terminated; pre-created ones terminate on their own
    if (clusterCreated && !properties.getDevelopment().isPreventClusterTermination()) {
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
    // per platform version, so parallel matrix jobs never rewrite a script another job is running
    String dbfsScriptPath =
        "dbfs:/databricks/openlineage/scripts/" + platformVersion + "/" + scriptName;
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
    // the scripts use it as the suffix of their table names, paths and events file
    task.setParameters(Collections.singletonList(platformVersion));

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
      String clusterName, String sparkVersionKey) {
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
            .setSparkVersion(sparkVersionKey)
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

  /**
   * Copies the jar to the cluster's own DBFS directory, from where the initialization script copies
   * it to the driver host. The copying happens only when the cluster starts, so the cluster has to
   * be restarted to use a new jar.
   */
  @SneakyThrows
  private void uploadOpenLineageJar(String clusterName) {
    Path jarFile =
        Files.list(Paths.get("../build/libs/"))
            .filter(p -> p.getFileName().toString().startsWith("openlineage-spark_"))
            .filter(p -> p.getFileName().toString().endsWith("-SNAPSHOT.jar"))
            .findAny()
            .orElseThrow(() -> new RuntimeException("openlineage-spark jar not found"));

    String jarDir = DBFS_CLUSTER_JARS + clusterName + "/";
    dbfs.mkdirs(jarDir);

    // the init script loads every OpenLineage jar in the directory, so remove the old ones
    for (FileInfo f : dbfs.list(jarDir)) {
      String path = f.getPath();
      if (path.contains("openlineage-spark") && path.endsWith(".jar")) {
        log.info("Deleting the old jar [{}]", path);
        dbfs.delete(path);
      }
    }

    uploadFileToDbfs(jarFile, jarDir + jarFile.getFileName());
  }

  /**
   * Uploads the cluster initialization script to the workspace.
   *
   * <p>The script copies the OpenLineage jar to the location where it is loaded by the driver. Its
   * stage directory is changed to the cluster's own directory, see {@link #DBFS_CLUSTER_JARS}.
   * {@code DB_CLUSTER_NAME} is set by Databricks when the script runs.
   */
  private void uploadInitializationScript() throws IOException {
    String script =
        Resources.toString(
            Paths.get("../databricks/open-lineage-init-script.sh").toUri().toURL(),
            StandardCharsets.UTF_8);
    if (!script.contains(INIT_SCRIPT_STAGE_DIR)) {
      throw new IllegalStateException(
          "The init script no longer contains [" + INIT_SCRIPT_STAGE_DIR + "]");
    }
    String clusterScript =
        script
            .replace(INIT_SCRIPT_STAGE_DIR, INIT_SCRIPT_CLUSTER_STAGE_DIR)
            .replace("$STAGE_DIR/", "\"$STAGE_DIR\"/");
    String encodedString =
        Base64.getEncoder().encodeToString(clusterScript.getBytes(StandardCharsets.UTF_8));
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
  private void uploadFileToDbfs(Path jarFile, String targetLocation) {
    String tempLocation = targetLocation + UUID.randomUUID() + ".tmp";

    // copy to a temp location first, so the cluster never sees a partially uploaded jar
    try (FileInputStream fis = new FileInputStream(jarFile.toString());
        OutputStream outputStream = dbfs.getOutputStream(tempLocation)) {
      // upload to DBFS -> 12MB file upload need to go in chunks smaller than 1MB each
      byte[] buf = new byte[500000]; // approx 0.5MB
      int len = fis.read(buf);
      while (len != -1) {
        outputStream.write(buf, 0, len);
        outputStream.flush();
        len = fis.read(buf);
      }
    }
    dbfs.move(tempLocation, targetLocation);
    log.info("OpenLineage jar has been uploaded to [{}]", targetLocation);
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
    String eventsFileLocation = properties.getDevelopment().getEventsFileLocation();
    Files.createDirectories(Paths.get(eventsFileLocation));
    String eventsLocation =
        eventsFileLocation + "/" + executionTimestamp + "-" + scriptName + "-events.ndjson";
    log.info("Fetching events to [{}]", eventsLocation);
    writeLinesToFile(eventsLocation, lines);
    log.info("Events fetched.");
  }

  private static void writeLinesToFile(String eventsLocation, List<String> lines)
      throws IOException {
    Files.createDirectories(Paths.get(eventsLocation).getParent());
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

  @Data
  private static class ClusterInitialization {
    private final String clusterId;
    private final boolean successful;
  }
}
