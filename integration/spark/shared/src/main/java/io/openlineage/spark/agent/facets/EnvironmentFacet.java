/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Map;

/**
 * Facet used to report environment specific properties. For example, reporting the name of the
 * cluster used, reporting certain environment variables, or resolving mount points.
 */
public class EnvironmentFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("environment-properties")
  @SuppressWarnings("PMD")
  private Map<String, Object> properties;

  public Map<String, Object> getProperties() {
    return properties;
  }

  public EnvironmentFacet(Map<String, Object> environmentDetails) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.properties = environmentDetails;
  }
}
