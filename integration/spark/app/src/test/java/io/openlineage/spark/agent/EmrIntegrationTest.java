/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 *
 *
 * <h1>EMR Integration Tests</h1>
 *
 * This suite of integration tests verifies OpenLineage's functionality on AWS EMR. These tests
 * automatically launch a new EMR cluster, upload the necessary OpenLineage JARs and scripts to S3,
 * execute those scripts, capture the resulting events, and validate them. After completion, the
 * cluster is terminated, and any temporary files are deleted.
 *
 * <p>The tests are meant to include only the cases which are difficult to catch with alternative
 * methods like unit tests or Spark integration tests in the container.
 *
 * <p>To execute the tests, configure the required parameters using system properties (refer to
 * {@link EmrDynamicParameter} for more details).
 *
 * <h2>PySpark Test Samples</h2>
 *
 * <h3>Templates</h3>
 *
 * <p>The test scripts use a templating system where parameters are injected via double curly braces
 * ({@code {{parameterName}}}). We recommend defining parameters as constants at the top of the
 * script for easier reference throughout the code.
 *
 * <h3>Handling Event Processing</h3>
 *
 * <p>Since the events are processed by a daemon thread, it's essential to add a brief sleep period
 * at the end of the script (3 seconds is enough). This ensures that all the Spark events are
 * processed before the application terminates.
 *
 * <h2>Infrastructure Requirements</h2>
 *
 * <p>To run, the tests require infrastructure. Make sure the following are available before you run
 * the tests:
 *
 * <ul>
 *   <li>An S3 for storing test files
 *   <li>An EC2 instance profile
 *   <li>An IAM role assigned to the EMR cluster
 * </ul>
 *
 * <h2>Configuration</h2>
 *
 * <p>All infrastructure details and configuration parameters should be set using system properties.
 * For example: {@code -Dopenlineage.tests.bucketName=my-bucket-name}. Most parameters have
 * defaults. For a full list of configurable parameters, see {@link EmrDynamicParameter}.
 *
 * <h3>Note on JUnit and Gradle</h3>
 *
 * <p>JUnit runs tests in a separate JVM. When using Gradle, system properties passed as {@code
 * -Dparameter=value} are not automatically available in the test JVMs. Gradle script is configured
 * to pass system properties prefixed with {@code openlineage}, so only those can be used safely in
 * tests.
 */
@Tag("integration-test")
@Tag("aws")
class EmrIntegrationTest {

  private static final EmrTestEnvironment.EmrTestEnvironmentProperties emrTestParameters;

  static {
    // Tests prefix with the date mark to tell when they were run in UTC
    String testsPrefix =
        EmrDynamicParameter.TestsKeyPrefix.resolve()
            + ZonedDateTime.now(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
            + "/";
    String clusterId = EmrDynamicParameter.ClusterId.resolve();
    EmrTestEnvironment.EmrTestEnvironmentProperties.NewCluster newCluster =
        "".equals(clusterId)
            ? EmrTestEnvironment.EmrTestEnvironmentProperties.NewCluster.builder()
                .emrLabel(EmrDynamicParameter.EmrLabel.resolve())
                .ec2InstanceProfile(EmrDynamicParameter.Ec2InstanceProfile.resolve())
                .serviceRole(EmrDynamicParameter.ServiceRole.resolve())
                .masterInstanceType(EmrDynamicParameter.MasterInstanceType.resolve())
                .slaveInstanceType(EmrDynamicParameter.SlaveInstanceType.resolve())
                .subnetId(EmrDynamicParameter.Ec2SubnetId.resolve())
                .ec2SshKeyName(EmrDynamicParameter.SshKeyPairName.resolve())
                .idleClusterTerminationSeconds(
                    Long.parseLong(EmrDynamicParameter.IdleClusterTerminationSeconds.resolve()))
                .build()
            : null;
    emrTestParameters =
        EmrTestEnvironment.EmrTestEnvironmentProperties.builder()
            .development(
                EmrTestEnvironment.EmrTestEnvironmentProperties.Development.builder()
                    // We can connect to the existing EMR cluster to speed up testing
                    .clusterId(clusterId)
                    .preventS3Cleanup(
                        Boolean.parseBoolean(EmrDynamicParameter.PreventS3Cleanup.resolve()))
                    .preventClusterTermination(
                        Boolean.parseBoolean(
                            EmrDynamicParameter.PreventClusterTermination.resolve()))
                    .debugPort(Integer.parseInt(EmrDynamicParameter.DebugPort.resolve()))
                    .build())
            .cluster(newCluster)
            .bucketName(EmrDynamicParameter.BucketName.resolve())
            .keyPrefix(testsPrefix)
            .build();
  }

  private static final EmrTestEnvironment emrTestEnvironment =
      new EmrTestEnvironment(emrTestParameters);

  @BeforeAll
  public static void setup() {}

  @AfterAll
  public static void teardown() {
    emrTestEnvironment.s3Cleanup();
    emrTestEnvironment.close();
  }

  @Test
  void testBasicScriptHasOutputs() {
    List<OpenLineage.RunEvent> runEvents =
        emrTestEnvironment.runScript(
            "basic_script.py",
            Map.of(
                "bucketName",
                emrTestParameters.getBucketName(),
                "outputPrefix",
                emrTestParameters.getKeyPrefix() + "output",
                "namespace",
                "someNamespace"));

    assertThat(runEvents).isNotEmpty();

    OpenLineage.RunEvent completeEvent =
        runEvents.stream()
            .filter(runEvent -> !runEvent.getOutputs().isEmpty())
            .filter(runEvent -> runEvent.getEventType() == OpenLineage.RunEvent.EventType.COMPLETE)
            .findFirst()
            .get();

    assertThat("someNamespace").isEqualTo(completeEvent.getJob().getNamespace());
    assertThat("s3://" + emrTestParameters.getBucketName())
        .isEqualTo(completeEvent.getOutputs().get(0).getNamespace());
  }

  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  @Test
  void testImplicitGlueCatalogIsUsed() {
    /*
     * The Aws Glue integration in EMR connects to the Glue catalog with the identifier specified by property
     * "hive.metastore.glue.catalogid". When it is not provided, we still want to use Glue and receive
     * the correct symlinks. In such scenarios the account ID of the current AWS account will be used as catalog ID.
     */
    String bucketName = emrTestParameters.getBucketName();
    String outputPrefix = emrTestParameters.getKeyPrefix() + "output";
    String databaseName = "peopleDatabase";
    String glueDatabaseName = databaseName.toLowerCase(Locale.ROOT);
    String databaseLocation = outputPrefix + "/" + databaseName;
    String inputTableName = "peopleSource";
    String glueInputTableName = inputTableName.toLowerCase(Locale.ROOT);
    String outputTableName = "peopleDestination";
    String glueOutputTableName = outputTableName.toLowerCase(Locale.ROOT);
    List<OpenLineage.RunEvent> runEvents =
        emrTestEnvironment.runScript(
            "glue_symlink_implicit_account_id.py",
            Map.of(
                "bucketName", bucketName,
                "outputPrefix", outputPrefix,
                "databaseName", databaseName,
                "sourceTableName", inputTableName,
                "destinationTableName", outputTableName));

    assertThat(runEvents).isNotEmpty();

    OpenLineage.RunEvent inOutEvent =
        runEvents.stream()
            .filter(runEvent -> runEvent.getEventType() == OpenLineage.RunEvent.EventType.COMPLETE)
            .filter(runEvent -> !runEvent.getOutputs().isEmpty())
            .filter(runEvent -> !runEvent.getInputs().isEmpty())
            .filter(
                runEvent ->
                // Has output symlink
                {
                  OpenLineage.SymlinksDatasetFacet symlinks =
                      runEvent.getOutputs().get(0).getFacets().getSymlinks();
                  return symlinks != null && !symlinks.getIdentifiers().isEmpty();
                })
            .filter(
                runEvent ->
                // Has input symlink
                {
                  OpenLineage.SymlinksDatasetFacet symlinks =
                      runEvent.getInputs().get(0).getFacets().getSymlinks();
                  return symlinks != null && !symlinks.getIdentifiers().isEmpty();
                })
            .findFirst()
            .get();

    OpenLineage.InputDataset inputDataset = inOutEvent.getInputs().get(0);
    OpenLineage.SymlinksDatasetFacetIdentifiers inputDatasetSymlink =
        inputDataset.getFacets().getSymlinks().getIdentifiers().get(0);

    assertThat(inputDataset.getNamespace()).isEqualTo("s3://" + bucketName);
    /*
      Note:
      The dataset name (table files' location) is formed by concatenating the database location and the table name.
      - The database location is case-sensitive.
      - Both the table name and database name are always converted to lowercase in AWS Glue.

      As a result, when combining them, the dataset name might contain a mix of uppercase (from the location)
      and lowercase characters (from the table name).
    */
    assertThat(inputDataset.getName()).isEqualTo(databaseLocation + "/" + glueInputTableName);
    assertThat(inputDatasetSymlink.getNamespace()).startsWith("arn:aws:glue:");
    assertThat(inputDatasetSymlink.getName())
        .isEqualTo("table/" + glueDatabaseName + "/" + glueInputTableName);
    assertThat(inputDatasetSymlink.getType()).isEqualTo("TABLE");

    OpenLineage.OutputDataset outputDataset = inOutEvent.getOutputs().get(0);
    OpenLineage.SymlinksDatasetFacetIdentifiers outputDatasetSymlink =
        outputDataset.getFacets().getSymlinks().getIdentifiers().get(0);
    assertThat(outputDataset.getNamespace()).isEqualTo("s3://" + bucketName);
    assertThat(outputDataset.getName()).isEqualTo(databaseLocation + "/" + glueOutputTableName);
    assertThat(outputDatasetSymlink.getNamespace()).startsWith("arn:aws:glue:");
    assertThat(outputDatasetSymlink.getName())
        .isEqualTo("table/" + glueDatabaseName + "/" + glueOutputTableName);
    assertThat(outputDatasetSymlink.getType()).isEqualTo("TABLE");
  }
}
