/*
/* Copyright 2018-2022 contributors to the OpenLineage project
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
}
