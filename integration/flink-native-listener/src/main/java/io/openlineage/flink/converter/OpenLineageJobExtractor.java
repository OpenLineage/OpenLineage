/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.JobFacetsBuilder;
import io.openlineage.client.OpenLineage.JobTypeJobFacetBuilder;
import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.config.FlinkOpenLineageConfig.JobConfig;
import io.openlineage.flink.config.FlinkOpenLineageConfig.JobOwnersConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

/** Class used to build job section of the OpenLineage events. */
class OpenLineageJobExtractor {
  public static final String FLINK_INTEGRATION = "FLINK";
  public static final String FLINK_JOB_TYPE = "JOB";
  public static final String BATCH = "BATCH";
  public static final String STREAMING = "STREAMING";

  private final OpenLineageContext context;

  OpenLineageJobExtractor(OpenLineageContext context) {
    this.context = context;
  }

  OpenLineage.Job extract(LineageGraph graph) {
    JobFacetsBuilder facetsBuilder = new JobFacetsBuilder();

    JobTypeJobFacetBuilder jobTypeJobFacetBuilder =
        context
            .getOpenLineage()
            .newJobTypeJobFacetBuilder()
            .jobType(FLINK_JOB_TYPE)
            .integration(FLINK_INTEGRATION);
    if (graph != null && graph.sources() != null) {
      jobTypeJobFacetBuilder.processingType(extractProcessingType(graph));
    }
    facetsBuilder.jobType(jobTypeJobFacetBuilder.build());
    buildOwnershipFacet(facetsBuilder);

    return context
        .getOpenLineage()
        .newJobBuilder()
        .namespace(context.getJobId().getJobNamespace())
        .name(context.getJobId().getJobNme())
        .facets(facetsBuilder.build())
        .build();
  }

  private JobFacetsBuilder buildOwnershipFacet(JobFacetsBuilder builder) {
    Optional.ofNullable(context.getConfig())
        .map(FlinkOpenLineageConfig::getJob)
        .map(JobConfig::getOwners)
        .map(JobOwnersConfig::getAdditionalProperties)
        .filter(Objects::nonNull)
        .ifPresent(
            map -> {
              List<OwnershipJobFacetOwners> ownersList = new ArrayList<>();
              map.forEach(
                  (type, name) ->
                      ownersList.add(
                          context
                              .getOpenLineage()
                              .newOwnershipJobFacetOwnersBuilder()
                              .name(name)
                              .type(type)
                              .build()));
              builder.ownership(
                  context
                      .getOpenLineage()
                      .newOwnershipJobFacetBuilder()
                      .owners(ownersList)
                      .build());
            });

    return builder;
  }

  private String extractProcessingType(LineageGraph graph) {
    return graph.sources().stream()
        .map(SourceLineageVertex::boundedness)
        .distinct()
        .filter(Objects::nonNull)
        .filter(b -> b.equals(Boundedness.CONTINUOUS_UNBOUNDED))
        .findAny()
        .map(a -> STREAMING)
        .orElse(BATCH);
  }
}
