/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.Transport;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class MetricsTest extends BaseMetricsTest {

  @SneakyThrows
  @Test
  void testSimpleMeterResolver() {
    Map<String, Object> config = new ObjectMapper().readValue("{\"type\": \"simple\"}", Map.class);
    MeterRegistry registry = MicrometerProvider.addMeterRegistryFromConfig(config);
    assertThat(registry)
        .isInstanceOfSatisfying(
            CompositeMeterRegistry.class,
            x -> {
              x.getRegistries().stream()
                  .findFirst()
                  .ifPresent(y -> assertThat(y).isInstanceOf(SimpleMeterRegistry.class));
            });
  }

  @SneakyThrows
  @Test
  void testGlobalRegistryBumpsMetrics() {
    OpenLineage openLineage = new OpenLineage(new URI("http://example.com"));
    Transport transport = mock(Transport.class);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerProvider.addMeterRegistry(registry);
    OpenLineageClient client =
        OpenLineageClient.builder()
            .transport(transport)
            .meterRegistry(MicrometerProvider.getMeterRegistry())
            .build();

    client.emit(
        openLineage.newRunEvent(
            ZonedDateTime.now(),
            OpenLineage.RunEvent.EventType.START,
            openLineage.newRun(UUID.randomUUID(), openLineage.newRunFacetsBuilder().build()),
            openLineage.newJob("namespace", "name", openLineage.newJobFacetsBuilder().build()),
            Collections.emptyList(),
            Collections.emptyList()));
    assertThat(registry.get("openlineage.emit.start").counter().count()).isEqualTo(1.0);
    assertThat(registry.get("openlineage.emit.complete").counter().count()).isEqualTo(1.0);
  }

  @SneakyThrows
  @Test
  void testTimerMeasuresTime() {
    OpenLineage openLineage = new OpenLineage(new URI("http://example.com"));
    Transport transport = mock(Transport.class);

    doAnswer(
            x -> {
              Thread.sleep(100);
              return null;
            })
        .when(transport)
        .emit(any(OpenLineage.RunEvent.class));

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerProvider.addMeterRegistry(registry);

    OpenLineageClient client =
        OpenLineageClient.builder()
            .transport(transport)
            .meterRegistry(MicrometerProvider.getMeterRegistry())
            .build();

    client.emit(openLineage.newRunEventBuilder().build());

    assertThat(registry.get("openlineage.emit.time").timer())
        .satisfies(timer -> assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(100));
  }
}
