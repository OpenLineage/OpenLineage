/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.testdata;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.UUID;

public final class OpenLineageEventsDataHelper {
  private OpenLineageEventsDataHelper() {}

  public static OpenLineage.RunEvent runEvent() {
    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newRunEventBuilder()
        .job(job)
        .run(run)
        .build();
  }

  public static OpenLineage.DatasetEvent datasetEvent() {
    OpenLineage.StaticDataset dataset =
        new OpenLineage.StaticDatasetBuilder()
            .namespace("test-namespace")
            .name("test-dataset")
            .build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newDatasetEventBuilder()
        .dataset(dataset)
        .build();
  }

  public static OpenLineage.JobEvent jobEvent() {
    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newJobEventBuilder()
        .job(job)
        .build();
  }
}
