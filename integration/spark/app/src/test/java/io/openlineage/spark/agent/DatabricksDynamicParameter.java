/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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

  // WORKSPACE PARAMETERS

  Host("workspace.host"),

  Token("workspace.token"),

  // CLUSTER PARAMETERS

  /** The Spark version as provided by Gradle. This case is not using the openlineage prefix. */
  SparkVersion("spark.version", null, "3.5.6"),

  // DEVELOPMENT PARAMETERS

  /**
   * The ID of the cluster to use. If specified, the tests will use this existing cluster instead of
   * creating a new one.
   */
  ClusterId("development.clusterId", ""),

  /**
   * When set to {@code true}, prevents the EMR cluster from terminating after tests complete. This
   * allows for manual inspection and debugging of the cluster state.
   */
  PreventClusterTermination("development.preventClusterTermination", "false"),

  /**
   * The location where the events should be stored for troubleshooting purposes. Each test has its
   * own file with execution timestamp as the prefix and the name of the script being executed.
   */
  EventsFileLocation("development.eventsFileLocation", "./build/events"),
  FetchEvents("development.fetchEvents", "true"),

  /**
   * When set to {@code true}, the given logs are fetched and stored under specified location. They
   * have the execution timestamp prefix added to the name of the file. It can take up to several
   * minutes for the logs to be available on DBFS, so you may consider keeping this function off if
   * you don't need them.
   */
  FetchLog4jLogs("development.logs.log4j.enabled", "true"),
  FetchStdout("development.logs.stdout.enabled", "true"),
  FetchStderr("development.logs.stderr.enabled", "true"),
  Log4jLogsLocation("development.logs.log4j.location", "./build/logs"),
  StdoutLocation("development.logs.stdout.location", "./build/logs"),
  StderrLocation("development.logs.stderr.location", "./build/logs");

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
