/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.flink.client.Versions;
import lombok.Getter;
import lombok.NonNull;

/** Captures information related to the Apache Flink job. */
@Getter
public class FlinkJobDetailsFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("jobId")
  @NonNull
  private String jobId;

  public FlinkJobDetailsFacet(@NonNull String jobId) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.jobId = jobId;
  }
}
