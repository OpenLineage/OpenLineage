/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.lifecycle;

import org.apache.spark.scheduler.*;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

public interface ExecutionContext {

  /**
   * Pattern used to match a variety of patterns in job names, including camel case and token
   * separated (whitespace, dash, or underscore), with special handling for upper-case
   * abbreviations, like XML or JDBC
   */
  String CAMEL_TO_SNAKE_CASE =
      "[\\s\\-_]?((?<=.)[A-Z](?=[a-z\\s\\-_])|(?<=[^A-Z])[A-Z]|((?<=[\\s\\-_])[a-z\\d]))";

  void setActiveJob(ActiveJob activeJob);

  void start(SparkListenerJobStart jobStart);

  void start(SparkListenerSQLExecutionStart sqlStart);

  void start(SparkListenerStageSubmitted stageSubmitted);

  void end(SparkListenerJobEnd jobEnd);

  void end(SparkListenerSQLExecutionEnd sqlEnd);

  void end(SparkListenerStageCompleted stageCompleted);
}
