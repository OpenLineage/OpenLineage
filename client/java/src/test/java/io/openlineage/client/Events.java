/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import java.net.URI;
import java.util.UUID;

public class Events {
  public static OpenLineage.RunEvent emptyRunEvent() {
    return new OpenLineage(URI.create("http://test.producer")).newRunEventBuilder().build();
  }

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

  public static OpenLineage.RunEvent runEventWithParent() {
    OpenLineage openLineage = new OpenLineage(URI.create("http://test.producer"));

    OpenLineage.ParentRunFacetJob parentJob =
        new OpenLineage.ParentRunFacetJobBuilder()
            .namespace("parent-namespace")
            .name("parent-job")
            .build();
    OpenLineage.ParentRunFacetRun parentRun =
        new OpenLineage.ParentRunFacetRunBuilder()
            .runId(UUID.fromString("d9cb8e0b-a410-435e-a619-da5e87ba8508"))
            .build();
    OpenLineage.ParentRunFacet parentRunFacet =
        openLineage.newParentRunFacetBuilder().job(parentJob).run(parentRun).build();
    OpenLineage.RunFacets runFacets =
        new OpenLineage.RunFacetsBuilder().parent(parentRunFacet).build();

    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run =
        new OpenLineage.RunBuilder()
            .runId(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54"))
            .facets(runFacets)
            .build();

    return openLineage.newRunEventBuilder().job(job).run(run).build();
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
