package io.openlineage.spark.agent.lifecycle;

import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;

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

  void end(SparkListenerJobEnd jobEnd);
}
