/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.emr.waiters.EmrWaiter;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The class that manages all the resources required to perform EMR integration tests. It provides
 * convenient methods for running scripts and retrieving the events. It creates the cluster,
 * terminates it, cleans up the S3 resources.
 */
@Slf4j
public class EmrTestEnvironment implements AutoCloseable {
  private final EmrClient client = EmrClient.builder().build();
  private final S3Client s3Client = S3Client.builder().build();
  private final EmrWaiter waiter = client.waiter();
  private final String openLineageJarKey;
  private final String s3TransportJarKey;

  /** The value is null when the cluster has not been started. */
  @Nullable private final String clusterId;

  private final EmrTestEnvironmentProperties properties;

  @Builder
  @Getter
  static class EmrTestEnvironmentProperties {
    @NonNull private final Development development;
    private final NewCluster cluster;
    // The name of the bucket used for jars, scripts and logs
    @NonNull private final String bucketName;
    // The unique prefix used to run the tests. It is the location where the files with jars,
    // scripts, events and logs will be stored
    @NonNull private final String keyPrefix;

    /** This class contains settings to facilitate development. */
    @Builder
    @Getter
    static class Development {
      // Optional ID of the cluster if we want to use the existing cluster instead of creating a new
      // one. If specified, the properties from NewCluster won't be used.
      // Empty String represents no value.
      private final String clusterId;
      // Optional flag preventing the cleanup process from S3. Useful when the results are different
      // from expected, and we want to investigate it
      private final boolean preventS3Cleanup;
      // Optional flag preventing the cluster shutdown at the end. Useful when you want to create
      // the
      // cluster
      // with test but want to keep it for future tests
      private final boolean preventClusterTermination;
      private final int debugPort;
    }

    @Builder
    @Getter
    static class NewCluster {
      @NonNull private final String emrLabel;
      @NonNull private final String serviceRole;
      @NonNull private final String ec2InstanceProfile;
      @NonNull private final String masterInstanceType;
      @NonNull private final String slaveInstanceType;
      @NonNull private final String subnetId;
      @NonNull private final String ec2SshKeyName;
      private final long idleClusterTerminationSeconds;
    }

    public String getJobsPrefix() {
      return keyPrefix + "jobs/";
    }

    public String getJarsPrefix() {
      return keyPrefix + "jars/";
    }

    public String getEventsPrefix() {
      return keyPrefix + "events/";
    }

    public String getEventsForScriptPrefix(String scriptName) {
      return getEventsPrefix() + scriptName + "/";
    }

    public String getLogsPrefix() {
      return keyPrefix + "logs/";
    }

    public String getScriptKey(String scriptName) {
      return keyPrefix + "scripts/" + scriptName;
    }
  }

  EmrTestEnvironment(EmrTestEnvironmentProperties properties) {
    this.properties = properties;
    String bucketName = properties.getBucketName();
    log.info(
        "Initiating EMR environment. The jars will be stores under [{}]. The logs will be stored under [{}]. The jobs will be stored under [{}]. The events will be stored under [{}]",
        AwsUtils.s3Url(bucketName, properties.getJarsPrefix()),
        AwsUtils.s3Url(bucketName, properties.getLogsPrefix()),
        AwsUtils.s3Url(bucketName, properties.getJobsPrefix()),
        AwsUtils.s3Url(bucketName, properties.getEventsPrefix()));

    this.openLineageJarKey =
        AwsUtils.uploadOpenLineageJar(s3Client, bucketName, properties.getJarsPrefix());
    this.s3TransportJarKey =
        AwsUtils.uploadS3TransportJar(s3Client, bucketName, properties.getJarsPrefix());

    // We can connect to the existing cluster. It can speed up testing. The existing cluster won't
    // be closed at the end.
    if (!properties.getDevelopment().getClusterId().isEmpty()) {
      log.info(
          "Attaching to the existing cluster [{}]", properties.getDevelopment().getClusterId());
      this.clusterId = properties.getDevelopment().getClusterId();
    } else {
      log.info("Creating a new EMR cluster");
      this.clusterId = createNewCluster(properties);
    }
  }

  /**
   * Runs the PySpark job from emr_test_job directory with the given name. Then retrieves the
   * emitted events.
   */
  List<OpenLineage.RunEvent> runScript(String scriptName, Map<String, String> parametersMap) {
    Map<String, String> parametersMapExtended = new HashMap<>(parametersMap);
    parametersMapExtended.put("eventsPrefix", properties.getEventsForScriptPrefix(scriptName));

    String scriptLocalPath = "emr_test_jobs/" + scriptName;
    String eventsForScriptPrefix = properties.getEventsForScriptPrefix(scriptName);
    String bucketName = properties.getBucketName();

    String scriptS3Location =
        uploadScriptToS3(scriptName, scriptLocalPath, parametersMapExtended, bucketName);

    submitJob(scriptName, bucketName, scriptS3Location);

    return AwsUtils.fetchEventsEmitted(s3Client, bucketName, eventsForScriptPrefix);
  }

  /** Renders script template and uploads it to S3 */
  private @NotNull String uploadScriptToS3(
      String scriptName,
      String scriptLocalPath,
      Map<String, String> parametersMap,
      String bucketName) {
    String scriptS3Key = properties.getScriptKey(scriptName);
    String scriptS3Location = AwsUtils.s3Url(bucketName, scriptS3Key);
    log.info("Uploading script [{}] to [{}]", scriptName, scriptS3Location);
    AwsUtils.uploadFile(
        s3Client,
        Templating.renderTemplate(scriptLocalPath, parametersMap),
        bucketName,
        scriptS3Key);
    log.info("The script [{}] has been uploaded to [{}].", scriptLocalPath, scriptS3Location);
    return scriptS3Location;
  }

  private void submitJob(String scriptName, String bucketName, String scriptS3Location) {
    // We attach OpenLineage and S3 transport jars
    String jars =
        String.join(
            ",",
            ImmutableList.of(
                AwsUtils.s3Url(bucketName, openLineageJarKey),
                AwsUtils.s3Url(bucketName, s3TransportJarKey)));

    log.info("Submitting step with the job.");
    AddJobFlowStepsResponse addJobFlowStepsResponse =
        client.addJobFlowSteps(
            AddJobFlowStepsRequest.builder()
                .jobFlowId(clusterId)
                .steps(
                    StepConfig.builder()
                        .name("run-" + scriptName)
                        .actionOnFailure(ActionOnFailure.CONTINUE)
                        .hadoopJarStep(
                            HadoopJarStepConfig.builder()
                                .jar("command-runner.jar")
                                .args(
                                    "spark-submit",
                                    "--jars",
                                    jars,
                                    "--driver-java-options",
                                    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:"
                                        + properties.getDevelopment().getDebugPort(),
                                    "--conf",
                                    "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener",
                                    "--conf",
                                    "spark.openlineage.transport.fileNamePrefix="
                                        + properties.getEventsForScriptPrefix(scriptName),
                                    "--conf",
                                    "spark.openlineage.transport.type=s3",
                                    "--conf",
                                    "spark.openlineage.transport.bucketName=" + bucketName,
                                    scriptS3Location)
                                .build())
                        .build())
                .build());
    String stepId = addJobFlowStepsResponse.stepIds().get(0);
    log.info("PySpark step submitted with ID [{}]. Waiting for completion...", stepId);
    waitForStepToComplete(stepId);
    log.info("PySpark step [{}] completed.", stepId);
  }

  void waitForStepToComplete(String stepId) {
    DescribeStepRequest describeStepRequest =
        DescribeStepRequest.builder().clusterId(clusterId).stepId(stepId).build();

    ResponseOrException<DescribeStepResponse> matched =
        waiter
            .waitUntilStepComplete(
                describeStepRequest,
                WaiterOverrideConfiguration.builder()
                    .backoffStrategyV2(BackoffStrategy.fixedDelay(Duration.ofSeconds(5)))
                    .maxAttempts(120)
                    .waitTimeout(Duration.ofMinutes(10))
                    .build())
            .matched();

    matched
        .response()
        .ifPresent(
            response -> {
              StepStatus stepStatus = response.step().status();
              log.info("Step [{}] completed with status: [{}]", stepId, stepStatus);
            });

    matched
        .exception()
        .ifPresent(
            e -> {
              throw new RuntimeException(
                  "The step [" + stepId + "] did not complete successfully", e);
            });
  }

  public void s3Cleanup() {
    if (properties.getDevelopment().isPreventS3Cleanup()) {
      log.info(
          "The [{}] flag has been enabled. Skipping S3 cleanup. Remember to remove it manually.",
          EmrDynamicParameter.PreventS3Cleanup.getParameterName());
    } else {
      log.info("Deleting the files under [{}]", properties.getKeyPrefix());
      AwsUtils.deleteFiles(s3Client, properties.getBucketName(), properties.getKeyPrefix());
    }
  }

  @Override
  public void close() {
    // We close the cluster only when the cluster has been created by the tests.
    // We can still prevent shutting down if the developer asks so.
    if (clusterId != null && properties.getDevelopment().getClusterId() == null) {
      if (!properties.getDevelopment().isPreventClusterTermination()) {
        client.terminateJobFlows(TerminateJobFlowsRequest.builder().jobFlowIds(clusterId).build());
        waitForClusterTerminated(clusterId);
      } else {
        log.warn("Preventing shutting down the cluster. Make sure you terminate it manually.");
      }
    }
    log.info("Closing EMR client");
    client.close();
    log.info("Closing S3 client");
    s3Client.close();
  }

  private String createNewCluster(EmrTestEnvironmentProperties properties) {
    EmrTestEnvironmentProperties.NewCluster cluster = properties.getCluster();

    RunJobFlowRequest request =
        RunJobFlowRequest.builder()
            .name("OpenLineageIntegrationTest")
            .releaseLabel(cluster.getEmrLabel())
            .applications(Application.builder().name("Spark").build())
            .logUri(AwsUtils.s3Url(properties.getBucketName(), properties.getLogsPrefix()))
            .configurations(
                Configuration.builder()
                    .classification("hive-site")
                    .properties(
                        ImmutableMap.of(
                            "hive.metastore.client.factory.class",
                            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                            "hive.execution.engine",
                            "spark"))
                    .build())
            .autoTerminationPolicy(
                AutoTerminationPolicy.builder()
                    .idleTimeout(cluster.getIdleClusterTerminationSeconds())
                    .build())
            .instances(
                JobFlowInstancesConfig.builder()
                    .instanceCount(1)
                    .keepJobFlowAliveWhenNoSteps(true) // Cluster doesn't shut down immediately
                    .masterInstanceType(cluster.getMasterInstanceType())
                    .slaveInstanceType(cluster.getSlaveInstanceType())
                    .ec2SubnetId(cluster.getSubnetId())
                    .ec2KeyName(cluster.ec2SshKeyName)
                    .build())
            .jobFlowRole(cluster.getEc2InstanceProfile())
            .serviceRole(cluster.getServiceRole())
            .build();
    String clusterId = client.runJobFlow(request).jobFlowId();
    waitForClusterReady(clusterId);
    return clusterId;
  }

  private void waitForClusterReady(String clusterId) {
    log.info("Waiting for cluster [{}] ready...", clusterId);
    // The default waiting strategy is to poll the cluster for 30 minutes (max 60 times) with around
    // 30 seconds between each attempt until the cluster says it is ready.
    ResponseOrException<DescribeClusterResponse> waiterResponse =
        waiter
            .waitUntilClusterRunning(DescribeClusterRequest.builder().clusterId(clusterId).build())
            .matched();

    waiterResponse.response().ifPresent(response -> log.info("Cluster [{}] is ready", clusterId));

    waiterResponse
        .exception()
        .ifPresent(
            e -> {
              throw new RuntimeException("Cluster didn't reach the expected state", e);
            });
  }

  private void waitForClusterTerminated(String clusterId) {
    log.info("Terminating cluster {}", clusterId);
    // The default waiting strategy is to poll the cluster for 30 minutes (max 60 times) with around
    // 30 seconds between each attempt until the cluster says it is ready.
    ResponseOrException<DescribeClusterResponse> waiterResponse =
        waiter
            .waitUntilClusterTerminated(
                DescribeClusterRequest.builder().clusterId(clusterId).build())
            .matched();

    waiterResponse
        .response()
        .ifPresent(response -> log.info("Cluster [{}] has been terminated", clusterId));

    waiterResponse
        .exception()
        .ifPresent(
            e -> {
              throw new RuntimeException("Cluster did not terminate successfully", e);
            });
  }
}
