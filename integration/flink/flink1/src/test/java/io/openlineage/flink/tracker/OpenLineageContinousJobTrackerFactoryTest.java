/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.junit.jupiter.api.Test;

class OpenLineageContinousJobTrackerFactoryTest {

  @Test
  void getTrackerBuildsJobsApiUrlFromConfiguredRestPortWhenNoOverrideIsSet() {
    Configuration configuration = new Configuration();
    configuration.set(RestOptions.ADDRESS, "localhost");
    configuration.set(RestOptions.PORT, 8080);

    OpenLineageContinousJobTracker tracker =
        OpenLineageContinousJobTrackerFactory.getTracker(configuration, Duration.ofSeconds(10));

    assertThat(tracker).extracting("jobsApiUrl").isEqualTo("http://localhost:8080/jobs");
  }

  @Test
  void getTrackerUsesCheckpointTrackingRestUrlOverrideWhenEffectivePortDiffersFromConfiguredPort() {
    // Reproduces issue #4739: rest.port is configured as 8080, but Flink's Application mode
    // binds the actual REST endpoint to a different, randomly-assigned port (44193 here). Before
    // the fix, getTracker() ignored this and always built the URL from the configured rest.port,
    // so the tracker polled the wrong endpoint.
    Configuration configuration = new Configuration();
    configuration.set(RestOptions.ADDRESS, "localhost");
    configuration.set(RestOptions.PORT, 8080);
    configuration.setString(
        FlinkRestApiUrlResolver.CHECKPOINT_TRACKING_REST_URL.key(), "http://localhost:44193");

    OpenLineageContinousJobTracker tracker =
        OpenLineageContinousJobTrackerFactory.getTracker(configuration, Duration.ofSeconds(10));

    assertThat(tracker).extracting("jobsApiUrl").isEqualTo("http://localhost:44193/jobs");
  }
}
