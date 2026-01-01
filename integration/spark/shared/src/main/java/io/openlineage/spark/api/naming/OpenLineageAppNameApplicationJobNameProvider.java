/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api.naming;

import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides a job name if the property "spark.openlineage.appName" is specified. This method of
 * providing job name has the highest priority and should take precedence over any other method.
 */
@Slf4j
class OpenLineageAppNameApplicationJobNameProvider implements ApplicationJobNameProvider {
  @Override
  public boolean isDefinedAt(OpenLineageContext openLineageContext) {
    boolean appNameIsOverridden =
        openLineageContext.getOpenLineageConfig().getOverriddenAppName() != null;
    if (appNameIsOverridden) {
      log.debug("The property [spark.openlineage.appName] is set.");
    } else {
      log.debug("The property [spark.openlineage.appName] is not set.");
    }
    return appNameIsOverridden;
  }

  @Override
  public String getJobName(OpenLineageContext openLineageContext) {
    String overriddenAppName = openLineageContext.getOpenLineageConfig().getOverriddenAppName();
    log.debug(
        "Using the property [spark.openlineage.appName] value [{}] as the app name.",
        overriddenAppName);
    return overriddenAppName;
  }
}
