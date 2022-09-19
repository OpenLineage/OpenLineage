package io.openlineage.spark.agent.filters;

import org.apache.spark.scheduler.SparkListenerEvent;

public interface EventFilter {

  default boolean isDisabled(SparkListenerEvent event) {
    return false;
  }
}
