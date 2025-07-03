/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api.naming;

import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;

/**
 * The provider that uses the "spark.app.name" property to provide the job name. It also includes a
 * fallback strategy (default value) if even this method fails.
 */
@Slf4j
class SparkApplicationNameApplicationJobNameProvider implements ApplicationJobNameProvider {
  @Override
  public boolean isDefinedAt(OpenLineageContext openLineageContext) {
    /*
    Even if the required "spark.app.name" property is not set, this provider can return a fallback default value.
     */
    return true;
  }

  @Override
  public String getJobName(OpenLineageContext openLineageContext) {
    return openLineageContext.getApplicationName();
  }
}
