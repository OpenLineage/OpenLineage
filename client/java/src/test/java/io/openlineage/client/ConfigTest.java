/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.circuitBreaker.JavaRuntimeCircuitBreaker;
import io.openlineage.client.circuitBreaker.JavaRuntimeCircuitBreakerConfig;
import io.openlineage.client.circuitBreaker.SimpleMemoryCircuitBreaker;
import io.openlineage.client.circuitBreaker.SimpleMemoryCircuitBreakerConfig;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.NoopTransport;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class ConfigTest {
  @BeforeEach
  void clear() {
    MicrometerProvider.clear();
  }

  @Test
  void testLoadConfigFromYaml() throws URISyntaxException {
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider("config/http.yaml"));
    assertThat(client.transport).isInstanceOf(HttpTransport.class);
  }

  @Test
  void testDisableOverridesConfigFromYaml() throws URISyntaxException {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      when(Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED")).thenReturn("true");

      OpenLineageClient client = Clients.newClient(new TestConfigPathProvider("config/http.yaml"));
      assertThat(client.transport).isInstanceOf(NoopTransport.class);
    }
  }

  @Test
  void testWrongDoesNotDisableConfigFromYaml() throws URISyntaxException {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      when(Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED")).thenReturn("anything_else");

      OpenLineageClient client = Clients.newClient(new TestConfigPathProvider("config/http.yaml"));
      assertThat(client.transport).isInstanceOf(HttpTransport.class);
    }
  }

  @Test
  void testFacetsDisabledConfigFromYaml() throws URISyntaxException {
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider("config/facets.yaml"));

    assertThat(client.disabledFacets).contains("facet1", "facet2");
  }

  @Test
  void testJavaRuntimeCircuitBreakerConfigFromYaml() throws URISyntaxException {
    OpenLineageClient client =
        Clients.newClient(new TestConfigPathProvider("config/circuitBreaker1.yaml"));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(JavaRuntimeCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new JavaRuntimeCircuitBreakerConfig(13, 10));

    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(1000);

    client = Clients.newClient(new TestConfigPathProvider("config/circuitBreaker2.yaml"));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(JavaRuntimeCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new JavaRuntimeCircuitBreakerConfig(13, 7, 200));
    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(200);
  }

  @Test
  void testSimpleMemoryCircuitBreakerConfigFromYaml() throws URISyntaxException {
    OpenLineageClient client =
        Clients.newClient(new TestConfigPathProvider("config/circuitBreaker3.yaml"));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(SimpleMemoryCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new SimpleMemoryCircuitBreakerConfig(13));
    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(1000);

    client = Clients.newClient(new TestConfigPathProvider("config/circuitBreaker4.yaml"));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(SimpleMemoryCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new SimpleMemoryCircuitBreakerConfig(13, 200));
    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(200);
  }

  @Test
  void testSimpleMetricsConfigFromYaml() {
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider("config/metrics.yaml"));
    CompositeMeterRegistry meterRegistry = (CompositeMeterRegistry) client.meterRegistry;
    assertThat(meterRegistry.getRegistries())
        .hasOnlyElementsOfType(SimpleMeterRegistry.class)
        .hasSize(1);
  }

  @Test
  void testCompositeMetricsConfigFromYaml() {
    OpenLineageClient client =
        Clients.newClient(new TestConfigPathProvider("config/metrics-complex.yaml"));
    CompositeMeterRegistry meterRegistry = (CompositeMeterRegistry) client.meterRegistry;
    assertThat(meterRegistry.getRegistries().iterator().next())
        .isInstanceOfSatisfying(
            CompositeMeterRegistry.class,
            x -> {
              assertThat(new ArrayList<>(x.getRegistries()))
                  .hasSize(1)
                  .anyMatch(y -> y instanceof SimpleMeterRegistry);
            });
  }

  static class TestConfigPathProvider implements ConfigPathProvider {
    private final Path path;

    @SneakyThrows
    public TestConfigPathProvider(String path) {
      this.path = Paths.get(this.getClass().getClassLoader().getResource(path).toURI());
    }

    @Override
    public List<Path> getPaths() {
      return Collections.singletonList(this.path);
    }
  }
}
