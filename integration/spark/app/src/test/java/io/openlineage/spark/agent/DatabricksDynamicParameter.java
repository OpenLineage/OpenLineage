/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * Class for dynamic provisioning of parameters. In the current form it retrieves the values from
 * the system properties. They can be set using -Dopenlineage.tests.databricks.parameterName=value
 * when running the application.
 */
@Slf4j
@Getter
public enum DatabricksDynamicParameter implements DynamicParameter {

  // DEVELOPMENT PARAMETERS

  /**
   * The ID of the cluster to use. If specified, the tests will use this existing cluster instead of
   * creating a new one.
   */
  ClusterId("clusterId", ""),

  /**
   * When set to {@code true}, prevents the EMR cluster from terminating after tests complete. This
   * allows for manual inspection and debugging of the cluster state.
   */
  PreventClusterTermination("preventClusterTermination", "false"),

  // WORKSPACE PARAMETERS

  Host("host"),

  Token("token"),

  // CLUSTER PARAMETERS

  /** The Spark version as provided by Gradle. This case is not using the openlineage prefix. */
  SparkVersion("spark.version", null, "3.5.2");

  private final String parameterName;
  private final String defaultValue;
  private final String prefix;

  DatabricksDynamicParameter(String parameterName) {
    this(parameterName, null);
  }

  DatabricksDynamicParameter(String parameterName, @Nullable String defaultValue) {
    this(parameterName, "openlineage.tests.databricks", defaultValue);
  }

  DatabricksDynamicParameter(
      String parameterName, @Nullable String prefix, @Nullable String defaultValue) {
    this.parameterName = parameterName;
    this.defaultValue = defaultValue;
    this.prefix = prefix;
  }

  @Override
  public Logger getLog() {
    return log;
  }
}
