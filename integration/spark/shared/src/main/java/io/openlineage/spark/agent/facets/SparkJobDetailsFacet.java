/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Properties;
import lombok.Getter;
import lombok.NonNull;
import org.apache.spark.scheduler.SparkListenerJobStart;

/** Captures information related to the Apache Spark job. */
@Getter
public class SparkJobDetailsFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("jobId")
  @NonNull
  private Integer jobId;

  @JsonProperty("jobDescription")
  private String jobDescription;

  @JsonProperty("jobGroup")
  private String jobGroup;

  @JsonProperty("jobCallSite")
  private String jobCallSite;

  public SparkJobDetailsFacet(@NonNull SparkListenerJobStart jobStart) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.jobId = jobStart.jobId();

    Properties jobProperties = jobStart.properties();
    this.jobDescription = jobProperties.getProperty("spark.job.description");
    this.jobGroup = jobProperties.getProperty("spark.jobGroup.id");
    this.jobCallSite = jobProperties.getProperty("callSite.short");
  }
}
