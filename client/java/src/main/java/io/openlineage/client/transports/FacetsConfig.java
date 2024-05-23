/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class FacetsConfig implements MergeConfig<FacetsConfig> {

  @Getter
  @Setter
  @JsonProperty("disabled")
  private String[] disabledFacets;

  @Getter
  @JsonProperty("custom_environment_variables")
  private String[] customEnvironmentVariables;

  @Override
  public FacetsConfig mergeWithNonNull(FacetsConfig facetsConfig) {
    return new FacetsConfig(
        mergePropertyWith(disabledFacets, facetsConfig.disabledFacets),
        mergePropertyWith(customEnvironmentVariables, facetsConfig.customEnvironmentVariables));
  }
}
