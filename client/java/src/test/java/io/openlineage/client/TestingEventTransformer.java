/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import io.openlineage.client.OpenLineage.DatasetEvent;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.transports.EventTransformer;
import java.util.Map;

public class TestingEventTransformer implements EventTransformer {

  public Map<String, String> properties;

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public RunEvent transform(RunEvent event) {
    return null;
  }

  @Override
  public DatasetEvent transform(DatasetEvent event) {
    return null;
  }

  @Override
  public JobEvent transform(JobEvent event) {
    return null;
  }
}
