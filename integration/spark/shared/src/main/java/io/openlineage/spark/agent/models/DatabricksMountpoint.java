/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@JsonIgnoreProperties(ignoreUnknown = true)
@Value
public class DatabricksMountpoint {

  public DatabricksMountpoint(
      @JsonProperty("mountPointString") String mountPoint,
      @JsonProperty("sourceString") String source) {
    this.mountPoint = mountPoint;
    this.source = source;
  }

  String mountPoint;
  String source;
}
