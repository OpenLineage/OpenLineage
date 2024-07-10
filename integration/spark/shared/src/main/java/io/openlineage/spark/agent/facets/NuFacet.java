/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Properties;
import lombok.Getter;
import lombok.NonNull;
import io.openlineage.spark.api.OpenLineageContext;

/** Captures information related to the Apache Spark job. */
@Getter
public class NuFacet extends OpenLineage.DefaultRunFacet {
  // @JsonProperty("jobId")
  // @NonNull
  // private Integer jobId;

  // @JsonProperty("jobDescription")
  // private String jobDescription;

  @JsonProperty("jobNurn")
  private String jobNurn;

  public NuFacet(@NonNull OpenLineageContext olContext) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.jobNurn = olContext.getJobNurn();
  }
}
