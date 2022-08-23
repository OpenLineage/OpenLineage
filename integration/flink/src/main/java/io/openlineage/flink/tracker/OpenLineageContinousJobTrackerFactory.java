/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import java.time.Duration;
import org.apache.flink.configuration.ReadableConfig;

/**
 * Factory class to create instance of {@link OpenLineageContinousJobTracker}. It is included for
 * unittesting with no dependency injection mechanism provided.
 */
public class OpenLineageContinousJobTrackerFactory {

  /** Default duration between REST API calls. */
  static final Duration defaultTrackingInterval = Duration.ofSeconds(10);

  public static OpenLineageContinousJobTracker getTracker(
      ReadableConfig config, Duration duration) {
    return new OpenLineageContinousJobTracker(config, duration);
  }

  public static OpenLineageContinousJobTracker getTracker(ReadableConfig config) {
    return new OpenLineageContinousJobTracker(config, defaultTrackingInterval);
  }
}
