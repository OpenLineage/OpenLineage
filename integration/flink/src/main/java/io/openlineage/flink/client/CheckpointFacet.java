/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Custom facet to contain counters from Flink checkpoints. */
@Getter
@EqualsAndHashCode
public class CheckpointFacet extends OpenLineage.DefaultRunFacet {

  @JsonProperty("completed")
  private final int completed;

  @JsonProperty("failed")
  private final int failed;

  @JsonProperty("in-progress")
  private final int in_progress;

  @JsonProperty("restored")
  private final int restored;

  @JsonProperty("total")
  private final int total;

  public CheckpointFacet(int completed, int failed, int in_progress, int restored, int total) {
    super(EventEmitter.OPEN_LINEAGE_CLIENT_URI);
    this.completed = completed;
    this.failed = failed;
    this.in_progress = in_progress;
    this.restored = restored;
    this.total = total;
  }
}
