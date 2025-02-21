/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.time.ZonedDateTime;
import org.apache.flink.streaming.api.lineage.LineageGraph;

public class LineageGraphConverter {
  private final OpenLineageContext context;

  private final OpenLineageJobExtractor jobExtractor;

  private final OpenLineageDatasetExtractor datasetExtractor;

  public LineageGraphConverter(OpenLineageContext context, Flink2VisitorFactory visitorFactory) {
    this.context = context;
    this.jobExtractor = new OpenLineageJobExtractor(context);
    this.datasetExtractor = new OpenLineageDatasetExtractor(context, visitorFactory);
  }

  public RunEvent convert(LineageGraph graph, EventType eventType) {
    return context
        .getOpenLineage()
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .eventType(eventType)
        .job(jobExtractor.extract(graph))
        .run(
            context
                .getOpenLineage()
                .newRun(context.getRunUuid(), new OpenLineage.RunFacetsBuilder().build()))
        .inputs(datasetExtractor.extractInputs(graph))
        .outputs(datasetExtractor.extractOutputs(graph))
        .build();
  }
}
