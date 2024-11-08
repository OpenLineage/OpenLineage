/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.acceptance;

import static io.openlineage.hive.acceptance.AcceptanceTestConstants.ACCEPTANCE_TEST_TIMEOUT_IN_SECONDS;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.CLEAN_UP_BQ;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.CLEAN_UP_CLUSTER;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.CLEAN_UP_GCS;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.DATAPROC_ENDPOINT;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.EVENTS_LOG_FILE;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.INIT_ACTION_PATH;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.INIT_ACTION_SCRIPT;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.JAR_DIRECTORY;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.JAR_PREFIX;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.REGION;
import static io.openlineage.hive.acceptance.AcceptanceTestConstants.ZONE;
import static io.openlineage.hive.acceptance.AcceptanceTestUtils.createBqDataset;
import static io.openlineage.hive.acceptance.AcceptanceTestUtils.deleteBqDatasetAndTables;
import static io.openlineage.hive.acceptance.AcceptanceTestUtils.generateClusterName;
import static io.openlineage.hive.acceptance.AcceptanceTestUtils.generateTestId;
import static io.openlineage.hive.acceptance.AcceptanceTestUtils.readFileFromVM;
import static io.openlineage.hive.acceptance.AcceptanceTestUtils.uploadInitAction;
import static io.openlineage.hive.acceptance.AcceptanceTestUtils.uploadJar;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.datacatalog.lineage.v1.LineageEvent;
import com.google.cloud.datacatalog.lineage.v1.Process;
import com.google.common.collect.ImmutableMap;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.Cluster;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.ClusterConfig;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.ClusterControllerClient;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.ClusterControllerSettings;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.DiskConfig;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.GceClusterConfig;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.HiveJob;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.InstanceGroupConfig;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.Job;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.JobControllerClient;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.JobControllerSettings;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.JobPlacement;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.JobStatus;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.NodeInitializationAction;
import hive.acceptance.shaded.com.google.cloud.dataproc.v1.SoftwareConfig;
import io.openlineage.hive.TestsBase;
import io.openlineage.hive.testutils.DataplexTestUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DataprocAcceptanceTestBase {

  protected AcceptanceTestContext context;
  protected String olJobNamespace;
  protected Process dataplexProcess;

  protected abstract String getDataprocImageVersion();

  @BeforeAll
  protected void beforeAll() throws Exception {
    String dataprocImageVersion = getDataprocImageVersion();
    String testId = generateTestId(dataprocImageVersion);
    String clusterName = generateClusterName(testId);
    String testBaseGcsDir = AcceptanceTestUtils.createTestBaseGcsDir(testId);
    String jarUri = testBaseGcsDir + "/" + JAR_PREFIX + ".jar";
    String initActionUri = testBaseGcsDir + "/" + INIT_ACTION_SCRIPT;
    String bqProject = TestsBase.getProject();
    String bqDataset = "acceptance_dataset_" + testId.replace("-", "_");
    olJobNamespace =
        String.format(
            "acceptance_test_namespace_%d_%d",
            Long.MAX_VALUE - System.currentTimeMillis(), Long.MAX_VALUE - System.nanoTime());

    uploadJar(JAR_DIRECTORY, JAR_PREFIX, jarUri);

    uploadInitAction(INIT_ACTION_PATH, initActionUri);

    createBqDataset(bqProject, bqDataset);

    createClusterIfNeeded(clusterName, dataprocImageVersion, jarUri, initActionUri, olJobNamespace);

    context =
        new AcceptanceTestContext(
            testId,
            dataprocImageVersion,
            clusterName,
            testBaseGcsDir,
            jarUri,
            initActionUri,
            bqProject,
            bqDataset);
    System.out.print(context);
  }

  @AfterAll
  protected void afterAll() throws Exception {
    if (context == null) {
      System.out.println("Context is not initialized, skip cleanup.");
      return;
    }

    if (CLEAN_UP_CLUSTER) {
      deleteCluster(context.clusterId);
    } else {
      System.out.println("Skip deleting cluster: " + context.clusterId);
    }

    if (CLEAN_UP_GCS) {
      AcceptanceTestUtils.deleteGcsDir(context.testBaseGcsDir);
    } else {
      System.out.println("Skip deleting GCS dir: " + context.testBaseGcsDir);
    }

    if (CLEAN_UP_BQ) {
      deleteBqDatasetAndTables(context.bqDataset);
    } else {
      System.out.println("Skip deleting BQ dataset: " + context.bqDataset);
    }
  }

  @FunctionalInterface
  private interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
  }

  public Process getDataplexProcess() throws IOException {
    if (dataplexProcess == null) {
      return DataplexTestUtils.getProcessWithOLNamespace(
          TestsBase.getDataplexLocationName(), olJobNamespace);
    }
    return dataplexProcess;
  }

  protected static void createClusterIfNeeded(
      String clusterName,
      String dataprocImageVersion,
      String jarUri,
      String initActionUri,
      String olJobNamespace)
      throws Exception {
    Cluster clusterSpec =
        createClusterSpec(clusterName, dataprocImageVersion, jarUri, initActionUri, olJobNamespace);
    System.out.println("Cluster spec:\n" + clusterSpec);
    System.out.println("Creating cluster " + clusterName + " ...");
    runClusterCommand(
        client -> client.createClusterAsync(TestsBase.getProject(), REGION, clusterSpec).get());
  }

  protected static void deleteCluster(String clusterName) throws Exception {
    System.out.println("Deleting cluster " + clusterName + " ...");
    runClusterCommand(
        client -> client.deleteClusterAsync(TestsBase.getProject(), REGION, clusterName).get());
  }

  private static void runClusterCommand(ThrowingConsumer<ClusterControllerClient> command)
      throws Exception {
    try (ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(
            ClusterControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      command.accept(clusterControllerClient);
    }
  }

  private static Cluster createClusterSpec(
      String clusterName,
      String dataprocImageVersion,
      String jarUri,
      String initActionUri,
      String olJobNamespace) {
    return Cluster.newBuilder()
        .setClusterName(clusterName)
        .setProjectId(TestsBase.getProject())
        .setConfig(
            ClusterConfig.newBuilder()
                .addInitializationActions(
                    NodeInitializationAction.newBuilder()
                        .setExecutableFile(String.format(initActionUri, REGION)))
                .setGceClusterConfig(
                    GceClusterConfig.newBuilder()
                        .setNetworkUri("default")
                        .setZoneUri(ZONE)
                        .setInternalIpOnly(false)
                        .putMetadata("jar-urls", jarUri))
                .setMasterConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(1)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setWorkerConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(2)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setSoftwareConfig(
                    SoftwareConfig.newBuilder()
                        .setImageVersion(dataprocImageVersion)
                        .putProperties(
                            "hive:hive.exec.post.hooks",
                            "io.openlineage.hive.hooks.HiveOpenLineageHook")
                        .putProperties("hive:hive.openlineage.namespace", olJobNamespace)
                        .putProperties("hive:hive.openlineage.transport.type", "composite")
                        .putProperties(
                            "hive:hive.openlineage.transport.transports",
                            String.join(
                                "\n",
                                "[",
                                String.format(
                                    "{\"type\":\"file\",\"location\":\"%s\"},", EVENTS_LOG_FILE),
                                String.format(
                                    "{\"type\":\"gcplineage\",\"mode\":\"sync\",\"projectId\":\"%s\",\"location\":\"%s\"}",
                                    TestsBase.getProject(), TestsBase.getGcpLocation()),
                                "]"))))
        .build();
  }

  private Job createAndRunHiveJob(String testName, String queryFile) throws Exception {
    Job job = createHiveJob(testName, queryFile);
    System.out.print("Running hive job:\n" + job);
    return runAndWait(testName, job);
  }

  private Map<String, String> getTestScriptVariables(String testName) {
    return ImmutableMap.<String, String>builder()
        .put("BQ_PROJECT", context.project)
        .put("BQ_DATASET", context.bqDataset)
        .put("HIVE_TEST_TABLE", testName.replaceAll("-", "_") + "_table")
        .put("HIVE_OUTPUT_TABLE", testName.replaceAll("-", "_") + "_output")
        .put("HIVE_OUTPUT_DIR_URI", context.getOutputDirUri(testName))
        .build();
  }

  private String uploadQueryFile(String testName, String queryFile) throws Exception {
    String queryFileUri = context.getFileUri(testName, queryFile);
    AcceptanceTestUtils.uploadResourceToGcs("/acceptance/" + queryFile, queryFileUri, "text/x-sql");
    return queryFileUri;
  }

  private Job createHiveJob(String testName, String queryFile) throws Exception {
    String queryFileUri = uploadQueryFile(testName, queryFile);
    HiveJob.Builder hiveJobBuilder =
        HiveJob.newBuilder()
            .setQueryFileUri(queryFileUri)
            .putAllScriptVariables(getTestScriptVariables(testName));
    return Job.newBuilder()
        .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
        .setHiveJob(hiveJobBuilder)
        .build();
  }

  private Job runAndWait(String testName, Job job) throws Exception {
    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(
            JobControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      Job request = jobControllerClient.submitJob(TestsBase.getProject(), REGION, job);
      String jobId = request.getReference().getJobId();
      System.err.printf("%s job ID: %s%n", testName, jobId);
      CompletableFuture<Job> finishedJobFuture =
          CompletableFuture.supplyAsync(
              () ->
                  waitForJobCompletion(jobControllerClient, TestsBase.getProject(), REGION, jobId));
      return finishedJobFuture.get(ACCEPTANCE_TEST_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    }
  }

  private Job waitForJobCompletion(
      JobControllerClient jobControllerClient, String projectId, String region, String jobId) {
    while (true) {
      // Poll the service periodically until the Job is in a finished state.
      Job jobInfo = jobControllerClient.getJob(projectId, region, jobId);
      switch (jobInfo.getStatus().getState()) {
        case DONE:
        case CANCELLED:
        case ERROR:
          return jobInfo;
        default:
          try {
            // Wait a second in between polling attempts.
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
      }
    }
  }

  void verifyJobSucceeded(Job job) throws Exception {
    String driverOutput =
        AcceptanceTestUtils.readGcsFile(job.getDriverControlFilesUri() + "driveroutput.000000000");
    System.out.println("Driver output: " + driverOutput);
    System.out.println("Job status: " + job.getStatus().getState());
    if (job.getStatus().getState() != JobStatus.State.DONE) {
      throw new AssertionError(job.getStatus().getDetails());
    }
  }

  @Test
  public void testInsertIntoTable() throws Exception {
    Job job = createAndRunHiveJob("insert_into_table", "insert_into_table.sql");
    verifyJobSucceeded(job);
    // Check the OpenLineage JSON event
    String eventsAsString =
        readFileFromVM(context.project, ZONE, context.clusterId + "-m", EVENTS_LOG_FILE);
    assertThat(eventsAsString)
        .contains("{\"namespace\":\"hive\",\"name\":\"default.t1\",\"type\":\"TABLE\"}");
    assertThat(eventsAsString)
        .contains("{\"namespace\":\"hive\",\"name\":\"default.t2\",\"type\":\"TABLE\"}");
    // Check the Dataplex event
    Process process = getDataplexProcess();
    List<LineageEvent> dataplexEvents = DataplexTestUtils.getEventsForProcess(process.getName());
    assertThat(dataplexEvents).hasSize(1);
    assertThat(dataplexEvents.get(0).getLinksList()).hasSize(1);
    assertThat(dataplexEvents.get(0).getLinksList().get(0).getSource().getFullyQualifiedName())
        .endsWith("/t2");
    assertThat(dataplexEvents.get(0).getLinksList().get(0).getTarget().getFullyQualifiedName())
        .endsWith("/t1");
  }
}
