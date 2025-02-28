/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.Resources;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.net.URI;
import lombok.SneakyThrows;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlinkConfigParserTest {

  Configuration configuration;
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
  ConfigOption testCompressionOption =
      ConfigOptions.key("openlineage.transport.compression").stringType().noDefaultValue();

  ConfigOption facetXDisabled =
      ConfigOptions.key("openlineage.facets.facetX.disabled").booleanType().noDefaultValue();
  ConfigOption facetYDisabled =
      ConfigOptions.key("openlineage.facets.facetY.disabled").booleanType().noDefaultValue();

  @BeforeEach
  void beforeEach() {
    configuration = new Configuration();
  }

  @Test
  @SneakyThrows
  void testFlinkConfToTransportConfig() {
    configuration.set(transportTypeOption, "http");
    configuration.set(urlOption, "http://some-url");
    configuration.set(apiKeyOption, "some-api-key");
    configuration.set(authTypeOption, "api_key");
    configuration.set(testHeaderOption, "some-header");
    configuration.set(testCompressionOption, "gzip");

    FlinkOpenLineageConfig config = FlinkConfigParser.parse(configuration);

    assertTrue(config.getTransportConfig() instanceof HttpConfig);
    HttpConfig transportConfig = (HttpConfig) config.getTransportConfig();

    assertEquals(new URI("http://some-url"), transportConfig.getUrl());
    assertThat(transportConfig.getAuth().getToken()).contains("some-api-key");
    assertEquals("some-header", transportConfig.getHeaders().get("testHeader"));
    assertEquals(HttpConfig.Compression.GZIP, transportConfig.getCompression());
  }

  @Test
  void testFlinkConfArrayEntry() {
    configuration.set(transportTypeOption, "console");
    configuration.set(facetXDisabled, "true");
    configuration.set(facetYDisabled, "false");

    FlinkOpenLineageConfig config = FlinkConfigParser.parse(configuration);

    assertThat(config.getFacetsConfig().getEffectiveDisabledFacets()).containsExactly("facetX");
  }

  @Test
  void testConfigReadFromYamlFile() {
    String propertyBefore = System.getProperty("user.dir");
    System.setProperty("user.dir", Resources.getResource("config").getPath());

    FlinkOpenLineageConfig config = FlinkConfigParser.parse(new Configuration());
    System.setProperty("user.dir", propertyBefore);

    assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);

    HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();
    assertThat(httpConfig.getUrl().toString()).isEqualTo("http://localhost:1010");
    assertThat(httpConfig.getAuth().getToken()).isEqualTo("Bearer random_token");
  }

  @Test
  void testFlinkConfOverwritesYamlFile() {
    configuration.set(transportTypeOption, "http");
    configuration.set(urlOption, "http://some-url");

    String propertyBefore = System.getProperty("user.dir");
    System.setProperty("user.dir", Resources.getResource("config").getPath());

    FlinkOpenLineageConfig config = FlinkConfigParser.parse(configuration);
    System.setProperty("user.dir", propertyBefore);

    assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);

    HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();

    // URL overwritten by SparkConf
    assertThat(httpConfig.getUrl().toString()).isEqualTo("http://some-url");

    // API config from yaml file
    assertThat(httpConfig.getAuth().getToken()).isEqualTo("Bearer random_token");
  }
}
