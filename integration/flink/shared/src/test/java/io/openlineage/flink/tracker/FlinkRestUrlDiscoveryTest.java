/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.junit.jupiter.api.Test;

class FlinkRestUrlDiscoveryTest {

  @Test
  void testResolveJobsApiUrlUsesOverride() {
    Configuration configuration = new Configuration();
    configuration.set(RestOptions.ADDRESS, "configured-host");
    configuration.set(RestOptions.PORT, 8081);

    String jobsApiUrl =
        FlinkRestUrlDiscovery.resolveJobsApiUrl(configuration, "http://override-host:18081/");

    assertThat(jobsApiUrl).isEqualTo("http://override-host:18081/jobs");
  }

  @Test
  void testResolveJobsApiUrlFallsBackToConfiguredAddress() {
    Configuration configuration = new Configuration();
    configuration.set(RestOptions.ADDRESS, "configured-host");
    configuration.set(RestOptions.PORT, 8081);

    String jobsApiUrl = FlinkRestUrlDiscovery.resolveJobsApiUrl(configuration, null);

    assertThat(jobsApiUrl).startsWith("http://");
    assertThat(jobsApiUrl).endsWith("/jobs");
  }

  @Test
  void testReadListeningPortsFromProcReturnsNonNullSet() {
    Set<Integer> ports = FlinkRestUrlDiscovery.readListeningPortsFromProc();

    assertThat(ports).isNotNull();
  }

  @Test
  void testDiscoverViaMiniDispatcherReturnsEmptyWhenUnavailable() {
    Optional<String> discovered =
        FlinkRestUrlDiscovery.discoverViaMiniDispatcherRestEndpoint("localhost");

    assertThat(discovered).isEmpty();
  }
}
