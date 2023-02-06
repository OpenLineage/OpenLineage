/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.With;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@With
public class FacetsConfig {

  @Getter
  @JsonProperty("disabled")
  private String[] disabledFacets;

  @Getter
  @JsonProperty("custom_environment_variables")
  private String[] customEnvironmentVariables;
}
