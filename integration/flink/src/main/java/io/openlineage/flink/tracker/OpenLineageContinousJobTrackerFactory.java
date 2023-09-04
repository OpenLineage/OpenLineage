/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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

  public static OpenLineageContinousJobTracker getTracker(
      ReadableConfig config, Duration duration) {
    return new OpenLineageContinousJobTracker(config, duration);
  }
}
