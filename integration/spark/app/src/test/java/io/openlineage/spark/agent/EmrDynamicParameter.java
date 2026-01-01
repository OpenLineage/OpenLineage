/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * Enumeration of dynamic parameters used in the EMR integration tests. Each parameter corresponds
 * to a system property that can be set to customize test behavior.
 *
 * <p>Parameters can be specified when running the application using the {@code -D} JVM argument
 * syntax: {@code -Dopenlineage.test.<parameterName>=value}.
 *
 * <p>For example, to set the EMR cluster ID, you can use: {@code
 * -Dopenlineage.test.clusterId=your-cluster-id}
 *
 * <p>This enum includes parameters for development settings and cluster configuration, such as
 * preventing S3 cleanup, specifying instance types, and setting the bucket name for test artifacts.
 */
@Slf4j
@Getter
public enum EmrDynamicParameter implements DynamicParameter {

  // DEVELOPMENT PARAMETERS

  /**
   * The ID of the EMR cluster to use. If specified, the tests will use this existing cluster
   * instead of creating a new one.
   */
  ClusterId("clusterId", ""),

  /**
   * When set to {@code true}, prevents the cleanup of S3 resources after tests complete. Useful for
   * debugging purposes.
   */
  PreventS3Cleanup("preventS3Cleanup", "false"),

  /**
   * When set to {@code true}, prevents the EMR cluster from terminating after tests complete. This
   * allows for manual inspection and debugging of the cluster state.
   */
  PreventClusterTermination("preventClusterTermination", "false"),

  /**
   * Determines which port can be used to debug the application. For debugging to work, make sure
   * the EC2 subnet has the firewall rule, allowing you to access the master node using this port.
   * You have to edit the EC2 security group the cluster is attached to and add the TCP inbound
   * rule. Then you can use remote debugging option in your IDE (with this port and the master
   * node's IP address) to attach session. If attaching seems to keep forever, it means that the
   * firewall rule is not correct. If the server rejects the debugger's connection it means the
   * application is not running yet, and you should repeat the attempt or make sure it is still
   * running. You should run the cluster beforehand, note the master IP address and have the
   * debugging session prepared before you attach the
   */
  DebugPort("debugPort", "5005"),

  // CLUSTER
  EmrLabel("emrLabel", "emr-7.2.0"),

  /** The S3 key prefix where event logs will be stored. */
  EventsKeyPrefix("eventsKeyPrefix", "events"),

  Ec2InstanceProfile("ec2InstanceProfile", "EMR_EC2_DefaultRole"),

  ServiceRole("serviceRole", "EMR_DefaultRole"),

  MasterInstanceType("masterInstanceType", "m4.large"),

  SlaveInstanceType("slaveInstanceType", "m4.large"),

  Ec2SubnetId("ec2SubnetId"),

  /** The optional key pair which can be used to SSH to the cluster. Useful for troubleshooting. */
  SshKeyPairName("sshKeyPairName", ""),

  IdleClusterTerminationSeconds("clusterIdleTerminationSeconds", "300"),

  /**
   * The name of the S3 bucket where the tests store dependency JARs, scripts, produced events,
   * logs, etc.
   */
  BucketName("bucketName"),
  /**
   * The S3 key prefix under which the tests will be executed. Each test execution will have a
   * separate random directory inside this prefix.
   */
  TestsKeyPrefix("testsKeyPrefix", "emr-integration-tests/test-");

  private final String parameterName;
  private final String defaultValue;
  private final String prefix;

  EmrDynamicParameter(String parameterName) {
    this(parameterName, null);
  }

  EmrDynamicParameter(String parameterName, String defaultValue) {
    this.parameterName = parameterName;
    this.defaultValue = defaultValue;
    this.prefix = "openlineage.test";
  }

  @Override
  public Logger getLog() {
    return log;
  }
}
