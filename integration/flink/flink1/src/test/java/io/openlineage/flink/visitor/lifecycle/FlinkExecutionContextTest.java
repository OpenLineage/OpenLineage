/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.flink.api.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.client.CheckpointFacet;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class FlinkExecutionContextTest {

  Configuration config = new Configuration();

  JobIdentifier jobId =
      JobIdentifier.builder()
          .jobNamespace("jobNamespace")
          .jobName("jobName")
          .flinkJobId(new JobID(1, 2))
          .build();

  @AfterEach
  void cleanUp() {
    MicrometerProvider.clear();
  }

  @Test
  void testBuildEventForEventTypeWithJobOwnershipFacet() {
    ConfigOption transportTypeOption =
        ConfigOptions.key("openlineage.transport.type").mapType().noDefaultValue();

    config.setString(transportTypeOption, "console");
    config.setString("openlineage.job.owners.team", "MyTeam");
    config.setString("openlineage.job.owners.person", "John Smith");

    FlinkExecutionContext context =
        FlinkExecutionContextFactory.getContext(
            config, jobId, "streaming", Collections.emptyList());

    RunEvent runEvent = context.buildEventForEventType(EventType.COMPLETE).build();

    List<OwnershipJobFacetOwners> owners = runEvent.getJob().getFacets().getOwnership().getOwners();
    assertThat(owners).hasSize(2);
    assertThat(owners.stream().filter(o -> o.getType().equals("team")).findAny().get().getName())
        .isEqualTo("MyTeam");
    assertThat(owners.stream().filter(o -> o.getType().equals("person")).findAny().get().getName())
        .isEqualTo("John Smith");
  }

  @Test
  void testBuildEventForEventTypeWithNoJobOwnersConfig() {
    ConfigOption transportTypeOption =
        ConfigOptions.key("openlineage.transport.type").mapType().noDefaultValue();

    config.setString(transportTypeOption, "console");

    FlinkExecutionContext context =
        FlinkExecutionContextFactory.getContext(
            config, jobId, "streaming", Collections.emptyList());

    assertThat(
            context
                .buildEventForEventType(EventType.COMPLETE)
                .build()
                .getJob()
                .getFacets()
                .getOwnership())
        .isNull();
  }

  @Test
  void testEmitCheckpointEventIncrementsMetrics() {
    FlinkExecutionContext context = setupMetricsContext();
    context.onJobCheckpoint(new CheckpointFacet(1, 2, 3, 4, 5));

    assertThat(
            MicrometerProvider.getMeterRegistry()
                .counter("openlineage.flink.event.checkpoint.start")
                .count())
        .isGreaterThanOrEqualTo(1.0);
    assertThat(
            MicrometerProvider.getMeterRegistry()
                .counter("openlineage.flink.event.checkpoint.end")
                .count())
        .isGreaterThanOrEqualTo(1.0);
  }

  @Test
  void testEmitSubmittedEventIncrementsMetrics() {
    FlinkExecutionContext context = setupMetricsContext();
    context.onJobSubmitted();

    assertThat(
            MicrometerProvider.getMeterRegistry()
                .counter("openlineage.flink.event.submitted.start")
                .count())
        .isGreaterThanOrEqualTo(1.0);
    assertThat(
            MicrometerProvider.getMeterRegistry()
                .counter("openlineage.flink.event.submitted.end")
                .count())
        .isGreaterThanOrEqualTo(1.0);
  }

  @Test
  void testEmitCompletedEventIncrementsMetrics() {
    FlinkExecutionContext context = setupMetricsContext();
    context.onJobCompleted(mock(JobExecutionResult.class));

    assertThat(
            MicrometerProvider.getMeterRegistry()
                .counter("openlineage.flink.event.completed.start")
                .count())
        .isGreaterThanOrEqualTo(1.0);
    assertThat(
            MicrometerProvider.getMeterRegistry()
                .counter("openlineage.flink.event.completed.end")
                .count())
        .isGreaterThanOrEqualTo(1.0);
  }

  FlinkExecutionContext setupMetricsContext() {
    FlinkOpenLineageConfig config = mock(FlinkOpenLineageConfig.class);
    when(config.getTransportConfig()).thenReturn(new ConsoleConfig());
    when(config.getMetricsConfig()).thenReturn(Map.of("type", "simple"));
    return FlinkExecutionContextFactory.getContext(
        config, jobId, "streaming", mock(EventEmitter.class), Collections.emptyList());
  }
}
