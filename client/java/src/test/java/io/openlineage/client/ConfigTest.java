/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

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
