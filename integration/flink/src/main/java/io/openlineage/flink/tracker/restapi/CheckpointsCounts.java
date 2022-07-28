/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker.restapi;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
/**
 * Class representing Flink REST API for Counts sub element of response endpoint
 * /jobs/:jobid/checkpoints
 */
public class CheckpointsCounts {
  int completed;
  int failed;
  int in_progress;
  int restored;
  int total;
}
