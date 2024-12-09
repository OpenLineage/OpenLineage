/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage.DatasetEvent;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.Map;

/**
 * EventTransformer is an interface that defines the transformation of an event. An implementation
 * of this interface is required to instantiate transform interface.
 *
 * <p>Class implementing it needs to provide no-arg constructor
 */
public interface EventTransformer {

  /**
   * Initialize the transformer with properties.
   *
   * @param properties
   */
  default void initialize(Map<String, String> properties) {}

  /**
   * Transforms the RunEvent into a new RunEvent.
   *
   * @param event
   * @return
   */
  default RunEvent transform(RunEvent event) {
    return event;
  }

  /**
   * Transforms the DatasetEvent into a new DatasetEvent.
   *
   * @param event
   * @return
   */
  default DatasetEvent transform(DatasetEvent event) {
    return event;
  }

  /**
   * Transforms the JobEvent into a new JobEvent.
   *
   * @param event
   * @return
   */
  default JobEvent transform(JobEvent event) {
    return event;
  }
}
