/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.circuitBreaker.JavaRuntimeCircuitBreaker;
import io.openlineage.client.circuitBreaker.JavaRuntimeCircuitBreakerConfig;
import io.openlineage.client.circuitBreaker.SimpleMemoryCircuitBreaker;
import io.openlineage.client.circuitBreaker.SimpleMemoryCircuitBreakerConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.NoopTransport;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class ConfigTest {
  @Test
  void testLoadConfigFromYaml() throws URISyntaxException {
    Path configPath =
        Paths.get(this.getClass().getClassLoader().getResource("config/http.yaml").toURI());
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(configPath));
    assertThat(client.transport).isInstanceOf(HttpTransport.class);
  }

  @Test
  void testDisableOverridesConfigFromYaml() throws URISyntaxException {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      when(Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED")).thenReturn("true");

      Path configPath =
          Paths.get(this.getClass().getClassLoader().getResource("config/http.yaml").toURI());
      OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(configPath));
      assertThat(client.transport).isInstanceOf(NoopTransport.class);
    }
  }

  @Test
  void testWrongDoesNotDisableConfigFromYaml() throws URISyntaxException {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      when(Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED")).thenReturn("anything_else");

      Path configPath =
          Paths.get(this.getClass().getClassLoader().getResource("config/http.yaml").toURI());
      OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(configPath));
      assertThat(client.transport).isInstanceOf(HttpTransport.class);
    }
  }

  @Test
  void testFacetsDisabledConfigFromYaml() throws URISyntaxException {
    Path configPath =
        Paths.get(this.getClass().getClassLoader().getResource("config/facets.yaml").toURI());
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(configPath));

    assertThat(client.disabledFacets).contains("facet1", "facet2");
  }

  @Test
  void testJavaRuntimeCircuitBreakerConfigFromYaml() throws URISyntaxException {
    Path configPath =
        Paths.get(
            this.getClass().getClassLoader().getResource("config/circuitBreaker1.yaml").toURI());
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(configPath));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(JavaRuntimeCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new JavaRuntimeCircuitBreakerConfig(13, 10));

    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(1000);

    configPath =
        Paths.get(
            this.getClass().getClassLoader().getResource("config/circuitBreaker2.yaml").toURI());
    client = Clients.newClient(new TestConfigPathProvider(configPath));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(JavaRuntimeCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new JavaRuntimeCircuitBreakerConfig(13, 7, 200));
    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(200);
  }

  @Test
  void testSimpleMemoryCircuitBreakerConfigFromYaml() throws URISyntaxException {
    Path configPath =
        Paths.get(
            this.getClass().getClassLoader().getResource("config/circuitBreaker3.yaml").toURI());
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(configPath));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(SimpleMemoryCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new SimpleMemoryCircuitBreakerConfig(13));
    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(1000);

    configPath =
        Paths.get(
            this.getClass().getClassLoader().getResource("config/circuitBreaker4.yaml").toURI());
    client = Clients.newClient(new TestConfigPathProvider(configPath));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(SimpleMemoryCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new SimpleMemoryCircuitBreakerConfig(13, 200));
    assertThat(client.circuitBreaker.get().getCheckIntervalMillis()).isEqualTo(200);
  }

  static class TestConfigPathProvider implements ConfigPathProvider {
    private final Path path;

    public TestConfigPathProvider(Path path) {
      this.path = path;
    }

    @Override
    public List<Path> getPaths() {
      return Collections.singletonList(this.path);
    }
  }
}
