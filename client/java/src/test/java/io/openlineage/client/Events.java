/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import java.net.URI;

public class Events {

  @Deprecated
  public static OpenLineage.RunEvent event() {
    return runEvent();
  }

  public static OpenLineage.RunEvent runEvent() {
    return new OpenLineage(URI.create("http://test.producer")).newRunEventBuilder().build();
  }

  public static OpenLineage.DatasetEvent datasetEvent() {
    return new OpenLineage(URI.create("http://test.producer")).newDatasetEventBuilder().build();
  }

  public static OpenLineage.JobEvent jobEvent() {
    return new OpenLineage(URI.create("http://test.producer")).newJobEventBuilder().build();
  }
}
