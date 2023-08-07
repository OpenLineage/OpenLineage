/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineageYaml;
import io.openlineage.client.transports.HttpConfig;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

public class FlinkConfigParserTest {

  Configuration configuration = new Configuration();
  ConfigOption transportTypeOption =
      ConfigOptions.key("openlineage.transport.type").stringType().noDefaultValue();
  ConfigOption urlOption =
      ConfigOptions.key("openlineage.transport.url").stringType().noDefaultValue();
  ConfigOption apiKeyOption =
      ConfigOptions.key("openlineage.transport.auth.apiKey").stringType().noDefaultValue();
  ConfigOption authTypeOption =
      ConfigOptions.key("openlineage.transport.auth.type").stringType().noDefaultValue();
  ConfigOption testHeaderOption =
      ConfigOptions.key("openlineage.transport.headers.testHeader").stringType().noDefaultValue();

  ConfigOption disabledFacetsOption =
      ConfigOptions.key("openlineage.facets.disabled").stringType().noDefaultValue();

  @Test
  @SneakyThrows
  void testFlinkConfToTransportConfig() {
    configuration.set(transportTypeOption, "http");
    configuration.set(urlOption, "http://some-url");
    configuration.set(apiKeyOption, "some-api-key");
    configuration.set(authTypeOption, "api_key");
    configuration.set(testHeaderOption, "some-header");

    Optional<OpenLineageYaml> openLineageYaml = FlinkConfigParser.parse(configuration);
    assertTrue(openLineageYaml.isPresent());

    assertTrue(openLineageYaml.get().getTransportConfig() instanceof HttpConfig);
    HttpConfig transportConfig = (HttpConfig) openLineageYaml.get().getTransportConfig();

    assertEquals(new URI("http://some-url"), transportConfig.getUrl());
    assertThat(transportConfig.getAuth().getToken()).contains("some-api-key");
    assertEquals("some-header", transportConfig.getHeaders().get("testHeader"));
  }

  @Test
  void testFlinkConfArrayEntry() {
    configuration.set(transportTypeOption, "console");
    configuration.set(disabledFacetsOption, "[facet1;facet2]");

    Optional<OpenLineageYaml> openLineageYaml = FlinkConfigParser.parse(configuration);
    assertTrue(openLineageYaml.isPresent());

    assertThat(openLineageYaml.get().getFacetsConfig().getDisabledFacets()[0]).isEqualTo("facet1");
  }
}
