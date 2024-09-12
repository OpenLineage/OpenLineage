/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.circuitBreaker.ExecutorCircuitBreaker;
import io.openlineage.client.circuitBreaker.JavaRuntimeCircuitBreaker;
import io.openlineage.client.circuitBreaker.JavaRuntimeCircuitBreakerConfig;
import io.openlineage.client.circuitBreaker.SimpleMemoryCircuitBreaker;
import io.openlineage.client.circuitBreaker.SimpleMemoryCircuitBreakerConfig;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceResolverConfig;
import io.openlineage.client.dataset.namespace.resolver.PatternMatchingGroupNamespaceResolverConfig;
import io.openlineage.client.dataset.namespace.resolver.PatternNamespaceResolverConfig;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.transports.CompositeTransport;
import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.NoopTransport;
import io.openlineage.client.transports.Transport;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

  @SuppressWarnings({"unchecked", "PMD.AvoidAccessibilityAlteration"})
  @ParameterizedTest
  @ValueSource(strings = {"config/composite-array.yaml", "config/composite-map.yaml"})
  void testLoadCompositeTransportConfigFromYaml(String yamlFile)
      throws NoSuchFieldException, SecurityException, IllegalArgumentException,
          IllegalAccessException {
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(yamlFile));
    assertThat(client.transport).isInstanceOf(CompositeTransport.class);
    CompositeTransport compositeTransport = (CompositeTransport) client.transport;
    Field transportsField = compositeTransport.getClass().getDeclaredField("transports");
    transportsField.setAccessible(true);
    List<Transport> target = (List<Transport>) transportsField.get(compositeTransport);
    assertThat(target).hasSize(2);
    assertThat(target.get(0)).isInstanceOf(HttpTransport.class);
    assertThat(target.get(1)).isInstanceOf(ConsoleTransport.class);
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
    assertThat(((ExecutorCircuitBreaker) client.circuitBreaker.get()).getTimeout()).isEmpty();

    client = Clients.newClient(new TestConfigPathProvider("config/circuitBreaker2.yaml"));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(JavaRuntimeCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new JavaRuntimeCircuitBreakerConfig(13, 7, 200, 90));
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
    assertThat(((ExecutorCircuitBreaker) client.circuitBreaker.get()).getTimeout()).isEmpty();

    client = Clients.newClient(new TestConfigPathProvider("config/circuitBreaker4.yaml"));

    assertThat(client.circuitBreaker.get())
        .isInstanceOf(SimpleMemoryCircuitBreaker.class)
        .hasFieldOrPropertyWithValue("config", new SimpleMemoryCircuitBreakerConfig(13, 200, 90));
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
        Clients.newClient(new TestConfigPathProvider("config/metrics-composite.yaml"));
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

  @Test
  void testOverwriteConfig() {
    OpenLineageConfig base = new OpenLineageConfig();
    OpenLineageConfig overwrite = new OpenLineageConfig();

    base.setMetricsConfig(null);
    overwrite.setMetricsConfig(Collections.singletonMap("k", "v"));

    base.setTransportConfig(new HttpConfig());
    overwrite.setTransportConfig(new ConsoleConfig());

    OpenLineageConfig config = (OpenLineageConfig) base.mergeWith(overwrite);
    assertThat(config.getMetricsConfig()).hasSize(1);
    assertThat(config.getTransportConfig()).isInstanceOf(ConsoleConfig.class);
  }

  @Test
  void testOverwriteConfigOverwritesDeepTransport() {
    OpenLineageConfig base = new OpenLineageConfig();
    OpenLineageConfig overwrite = new OpenLineageConfig();

    HttpConfig baseHttpConfig = new HttpConfig();
    HttpConfig overwriteHttpConfig = new HttpConfig();

    baseHttpConfig.setEndpoint("endpoint1");
    overwriteHttpConfig.setEndpoint("endpoint2");

    base.setTransportConfig(baseHttpConfig);
    overwrite.setTransportConfig(overwriteHttpConfig);

    OpenLineageConfig config = (OpenLineageConfig) base.mergeWith(overwrite);
    assertThat(((HttpConfig) config.getTransportConfig()).getEndpoint()).isEqualTo("endpoint2");
  }

  @Test
  void testOverwriteDoesNotOverwriteCircuitBreakerWithDefaults() {
    OpenLineageConfig base = new OpenLineageConfig();
    OpenLineageConfig overwrite = new OpenLineageConfig();

    JavaRuntimeCircuitBreakerConfig baseCircuitBreaker = new JavaRuntimeCircuitBreakerConfig();
    JavaRuntimeCircuitBreakerConfig overwriteCircuitBreaker = new JavaRuntimeCircuitBreakerConfig();

    base.setCircuitBreaker(baseCircuitBreaker);
    overwrite.setCircuitBreaker(overwriteCircuitBreaker);

    baseCircuitBreaker.setMemoryThreshold(10); // non default setting
    overwriteCircuitBreaker.setMemoryThreshold(
        JavaRuntimeCircuitBreakerConfig.DEFAULT_MEMORY_THRESHOLD); // default setting

    base.mergeWith(overwrite);
    assertThat(((JavaRuntimeCircuitBreakerConfig) base.getCircuitBreaker()).getMemoryThreshold())
        .isEqualTo(10);
  }

  @Test
  void testDatasetNamespaceResolverConfig() {
    final OpenLineageConfig config =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            new TestConfigPathProvider("config/datasetNamespaceResolver.yaml"),
            new TypeReference<OpenLineageConfig>() {});

    assertThat(config.datasetConfig.getNamespaceResolvers()).hasSize(3);

    assertThat(config.datasetConfig.getNamespaceResolvers().get("kafka-prod"))
        .isInstanceOf(DatasetNamespaceResolverConfig.class)
        .hasFieldOrPropertyWithValue("schema", "kafka")
        .hasFieldOrPropertyWithValue(
            "hosts", Arrays.asList("kafka-prod13.company.com", "kafka-prod15.company.com"));

    assertThat(config.datasetConfig.getNamespaceResolvers().get("cassandra-prod"))
        .isInstanceOf(PatternNamespaceResolverConfig.class)
        .hasFieldOrPropertyWithValue("schema", "cassandra")
        .hasFieldOrPropertyWithValue("regex", "cassandra-prod(\\d)+\\.company\\.com");

    assertThat(config.datasetConfig.getNamespaceResolvers().get("test-pattern"))
        .isInstanceOf(PatternMatchingGroupNamespaceResolverConfig.class)
        .hasFieldOrPropertyWithValue("matchingGroup", "cluster")
        .hasFieldOrPropertyWithValue("schema", "cassandra")
        .hasFieldOrPropertyWithValue(
            "regex", "(?<cluster>[a-zA-Z-]+)-(\\d)+\\.company\\.com:[\\d]*");
  }
}
