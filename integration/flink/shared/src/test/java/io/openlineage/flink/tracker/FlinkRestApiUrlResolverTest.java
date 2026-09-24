/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;
import org.junit.jupiter.api.Test;

class FlinkRestApiUrlResolverTest {

  ReadableConfig config = mock(ReadableConfig.class);

  @Test
  void resolvesUrlFromConfiguredRestAddressAndPortWhenNoOverrideIsSet() {
    when(config.get(RestOptions.ADDRESS)).thenReturn("10.0.0.5");
    when(config.get(RestOptions.PORT)).thenReturn(8080);
    when(config.get(FlinkRestApiUrlResolver.CHECKPOINT_TRACKING_REST_URL)).thenReturn(null);

    // This reproduces the reported bug: when the effective REST port (e.g. 44193, randomly
    // bound by Flink in Application mode) differs from the configured rest.port (8080), and no
    // override is configured, the resolver falls back to the *configured* rest.port - which is
    // exactly the wrong endpoint in that scenario. Regression coverage for the override lives in
    // the next test.
    String url = FlinkRestApiUrlResolver.resolveJobsApiUrl(config);

    assertThat(url).isEqualTo("http://10.0.0.5:8080/jobs");
  }

  @Test
  void fallsBackToLocalhostWhenRestAddressIsNotConfigured() {
    when(config.get(RestOptions.ADDRESS)).thenReturn(null);
    when(config.get(RestOptions.PORT)).thenReturn(8081);
    when(config.get(FlinkRestApiUrlResolver.CHECKPOINT_TRACKING_REST_URL)).thenReturn(null);

    String url = FlinkRestApiUrlResolver.resolveJobsApiUrl(config);

    assertThat(url).isEqualTo("http://localhost:8081/jobs");
  }

  @Test
  void usesOverrideUrlWhenEffectiveRestPortDiffersFromConfiguredRestPort() {
    // Simulates Flink Application mode on Kubernetes where rest.port is configured as 8080 but
    // the JobManager actually binds to a random port (e.g. 44193) at runtime - the scenario
    // described in issue #4739. Configured rest.port must be ignored once an override is set.
    when(config.get(RestOptions.ADDRESS)).thenReturn("localhost");
    when(config.get(RestOptions.PORT)).thenReturn(8080);
    when(config.get(FlinkRestApiUrlResolver.CHECKPOINT_TRACKING_REST_URL))
        .thenReturn("http://localhost:44193");

    String url = FlinkRestApiUrlResolver.resolveJobsApiUrl(config);

    assertThat(url).isEqualTo("http://localhost:44193/jobs");
  }

  @Test
  void stripsTrailingSlashFromOverrideUrl() {
    when(config.get(FlinkRestApiUrlResolver.CHECKPOINT_TRACKING_REST_URL))
        .thenReturn("http://localhost:44193/");

    String url = FlinkRestApiUrlResolver.resolveJobsApiUrl(config);

    assertThat(url).isEqualTo("http://localhost:44193/jobs");
  }

  @Test
  void ignoresBlankOverrideAndFallsBackToConfiguredRestPort() {
    when(config.get(RestOptions.ADDRESS)).thenReturn("localhost");
    when(config.get(RestOptions.PORT)).thenReturn(8080);
    when(config.get(FlinkRestApiUrlResolver.CHECKPOINT_TRACKING_REST_URL)).thenReturn("   ");

    String url = FlinkRestApiUrlResolver.resolveJobsApiUrl(config);

    assertThat(url).isEqualTo("http://localhost:8080/jobs");
  }
}
