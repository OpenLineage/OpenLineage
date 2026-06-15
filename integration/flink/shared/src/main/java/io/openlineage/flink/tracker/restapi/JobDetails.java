/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker.restapi;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/** Class representing Flink REST API for endpoint /jobs/:jobid */
@Getter
@Setter
public class JobDetails {
  @JsonProperty("start-time")
  private Long startTime;
}
