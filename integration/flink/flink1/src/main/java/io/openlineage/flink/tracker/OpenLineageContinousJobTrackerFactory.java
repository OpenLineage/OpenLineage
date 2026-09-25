/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

/**
 * Factory class to create instance of {@link OpenLineageContinousJobTracker}. It is included for
 * unittesting with no dependency injection mechanism provided.
 */
public class OpenLineageContinousJobTrackerFactory {

  public static OpenLineageContinousJobTracker getTracker(
      ReadableConfig config, Duration duration) {

    String restApiBaseUrl = null;
    if (config instanceof Configuration) {
      FlinkOpenLineageConfig olConfig = FlinkConfigParser.parse((Configuration) config);
      if (olConfig != null) {
        restApiBaseUrl = olConfig.getRestApiBaseUrl();
      }
    }

    String jobsApiUrl = FlinkRestUrlDiscovery.resolveJobsApiUrl(config, restApiBaseUrl);

    return new OpenLineageContinousJobTracker(duration, jobsApiUrl);
  }
}
