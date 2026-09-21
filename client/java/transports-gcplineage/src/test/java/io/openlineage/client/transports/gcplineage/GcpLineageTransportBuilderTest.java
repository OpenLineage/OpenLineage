/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import datalineage.shaded.org.threeten.bp.Duration;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.transports.gcplineage.GcpLineageTransportConfig.Mode;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class GcpLineageTransportBuilderTest {

  @ParameterizedTest
  @CsvSource({"config/lowercase_async_config.yaml", "config/uppercase_async_config.yaml"})
  void buildFromYamlConfigLowercaseMode(String relativePath) throws URISyntaxException {
    Path path = Paths.get(this.getClass().getClassLoader().getResource(relativePath).toURI());

    OpenLineageConfig openLineageConfig =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            () -> Collections.singletonList(path), new TypeReference<OpenLineageConfig>() {});

    GcpLineageTransportConfig gcpLineageTransportConfig =
        (GcpLineageTransportConfig) openLineageConfig.getTransportConfig();
    assertEquals(Mode.ASYNC, gcpLineageTransportConfig.getMode());
  }

  @Test
  void buildFromYamlConfigWithRetry() throws URISyntaxException {
    Path path =
        Paths.get(this.getClass().getClassLoader().getResource("config/retry_config.yaml").toURI());

    OpenLineageConfig openLineageConfig =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            () -> Collections.singletonList(path), new TypeReference<OpenLineageConfig>() {});
    GcpLineageTransportConfig gcpLineageTransportConfig =
        (GcpLineageTransportConfig) openLineageConfig.getTransportConfig();
    assertEquals(Mode.ASYNC, gcpLineageTransportConfig.getMode());
    assertEquals(5, gcpLineageTransportConfig.getRetryConfig().getMaxAttempts());
    assertEquals(2, gcpLineageTransportConfig.getRetryConfig().getRetryDelayMultiplier());
    assertEquals(2, gcpLineageTransportConfig.getRetryConfig().getRpcTimeoutMultiplier());

    assertEquals(
        Duration.ofMillis(6000), gcpLineageTransportConfig.getRetryConfig().getTotalTimeout());
    assertEquals(
        Duration.ofMillis(1000), gcpLineageTransportConfig.getRetryConfig().getInitialRetryDelay());
    assertEquals(
        Duration.ofMillis(1000), gcpLineageTransportConfig.getRetryConfig().getMaxRetryDelay());
    assertEquals(
        Duration.ofMillis(1000), gcpLineageTransportConfig.getRetryConfig().getInitialRpcTimeout());
    assertEquals(
        Duration.ofMillis(1000), gcpLineageTransportConfig.getRetryConfig().getMaxRpcTimeout());
  }
}
