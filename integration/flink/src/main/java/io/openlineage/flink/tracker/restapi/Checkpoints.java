/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker.restapi;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
/** Class representing Flink REST API for endpoint /jobs/:jobid/checkpoints */
public class Checkpoints {
  CheckpointsCounts counts;
}
