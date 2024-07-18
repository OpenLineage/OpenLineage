/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.job.naming;

import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;

/**
 * The provider that uses the "spark.app.name" property to provide the job name. It also includes a
 * fallback strategy (default value) if even this method fails.
 */
@Slf4j
public class SparkApplicationNameApplicationJobNameProvider implements ApplicationJobNameProvider {
  /** The job name used if none of the providers returns any value */
  private static final String DEFAULT_JOB_NAME = "unknown";

  @Override
  public boolean isDefinedAt(OpenLineageContext openLineageContext) {
    /*
    Even if the required "spark.app.name" property is not set, this provider can return a fallback default value.
     */
    return true;
  }

  @Override
  public String getJobName(OpenLineageContext openLineageContext) {
    if (openLineageContext.getSparkContext().isPresent()) {
      SparkContext sparkContext = openLineageContext.getSparkContext().get();
      String appName = sparkContext.appName();
      if (appName != null) {
        if (!appName.trim().isEmpty()) {
          log.debug("Using [spark.app.name property] for the application name.");
          return appName;
        } else {
          log.warn("The [spark.app.name property] property is blank.");
        }
      }
    }
    log.warn(
        "Failed to obtain the application name. Using the default value [{}]. Set the [spark.app.name] or [spark.openlineage.appName] property.",
        DEFAULT_JOB_NAME);
    return DEFAULT_JOB_NAME;
  }
}
