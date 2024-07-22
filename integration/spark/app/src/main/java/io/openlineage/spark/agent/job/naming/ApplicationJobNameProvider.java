/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.job.naming;

import io.openlineage.spark.api.OpenLineageContext;

/**
 * There can be multiple ways how the job name can be obtained for OpenLineage initial event. It
 * depends on the environment the application is deployed in, the properties passed to the app, etc.
 * This interface defines the contract for each provider.
 */
public interface ApplicationJobNameProvider {

  /** Determines if this provider can provide a job name for the given OpenLineageContext. */
  boolean isDefinedAt(OpenLineageContext openLineageContext);

  /**
   * Returns the job name for the given OpenLineageContext. This method should only be called if
   * {@link #isDefinedAt(OpenLineageContext)} returns true.
   */
  String getJobName(OpenLineageContext openLineageContext);
}
