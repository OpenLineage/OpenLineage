/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Class for dynamic provisioning of parameters. In the current form it retrieves the values from
 * the system properties. They can be set using -Dparameter.name=value when running the application.
 */
@Slf4j
@Getter
public enum DynamicParameter {
  // DEVELOPMENT
  /**
   * The ID of the EMR cluster if we want to use the existing one instead of creating a new one in
   * the tests
   */
  ClusterId("clusterId", ""),
  PreventS3Cleanup("preventS3Cleanup", "false"),
  PreventClusterTermination("preventClusterTermination", "false"),

  // CLUSTER
  EmrLabel("emrLabel", "emr-7.2.0"),
  EventsKeyPrefix("eventsKeyPrefix", "events"),
  Ec2InstanceProfile("ec2InstanceProfile", "EMR_EC2_DefaultRole"),
  ServiceRole("serviceRole", "EMR_DefaultRole"),
  MasterInstanceType("masterInstanceType", "m4.large"),
  SlaveInstanceType("slaveInstanceType", "m4.large"),

  /** The bucket where the tests keep the dependency jars, scripts, produced events, logs, etc */
  BucketName("bucketName"),
  /**
   * The prefix where the tests will be run. Each test execution will have a separate random
   * directory inside.
   */
  TestsKeyPrefix("testsKeyPrefix", "emr-integration-tests/test-");

  private final String templateParameter;
  private final String defaultValue;

  DynamicParameter(String templateParameter) {
    this(templateParameter, null);
  }

  DynamicParameter(String templateParameter, String defaultValue) {
    this.templateParameter = templateParameter;
    this.defaultValue = defaultValue;
  }

  String resolve() {
    String key = "openlineage.tests." + getTemplateParameter();
    log.debug("Resolving parameter [{}] using key [{}]", name(), key);
    String resolved = System.getProperty(key);
    if (resolved != null) {
      return resolved;
    } else {
      if (defaultValue != null) {
        log.debug(
            "The value for parameter [{}] has not been found. Using the default value [{}]",
            key,
            defaultValue);
        return defaultValue;
      }
    }
    throw new RuntimeException(
        "The value ["
            + key
            + "] could not be found in the system properties. Use `-D"
            + key
            + "=YOUR_VALUE`");
  }
}
