/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import java.time.Duration;
import java.util.Optional;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;

/**
 * Factory class to create instance of {@link OpenLineageContinousJobTracker}. It is included for
 * unittesting with no dependency injection mechanism provided.
 */
public class OpenLineageContinousJobTrackerFactory {

  public static OpenLineageContinousJobTracker getTracker(
      ReadableConfig config, Duration duration) {

    String jobsApiUrl =
        String.format(
            "http://%s:%s/jobs",
            Optional.ofNullable(config.get(RestOptions.ADDRESS)).orElse("localhost"),
            config.get(RestOptions.PORT));

    return new OpenLineageContinousJobTracker(duration, jobsApiUrl);
  }
}
