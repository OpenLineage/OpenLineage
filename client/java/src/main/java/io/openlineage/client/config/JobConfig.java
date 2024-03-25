/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.With;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@With
public class JobConfig {

  /**
   * Maps ownership type onto the name of the owner. See @see <a
   * href="https://openlineage.io/docs/spec/facets/job-facets/ownership">facet definition</a>.
   *
   * <p>Passing job owners as a map (type -> name) provides some limitation to the facet, as only a
   * single owner of the given type is allowed. The purpose of this is to provide simplicity for the
   * users passing the value though integrations config entries like Spark conf or Flink conf.
   */
  @Getter
  @JsonProperty("owners")
  private Map<String, String> jobOwners;
}
