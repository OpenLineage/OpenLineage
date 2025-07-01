/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Slf4j
public class TimeoutConfig {

  /**
   * If a timeout is set within a circuit breaker, this configures a percentage of the configured
   * timeout that can be spent on building datasets.
   */
  public Integer buildDatasetsTimePercentage;

  /**
   * If a timeout is set within a circuit breaker, this configures a percentage of the configured
   * timeout that can be spent on building facets which includes job facets, run facets, and dataset
   * facets. It can be assumed that this timeout applies on everything besides event serialization
   * and transport.
   */
  public Integer facetsBuildingTimePercentage;
}
