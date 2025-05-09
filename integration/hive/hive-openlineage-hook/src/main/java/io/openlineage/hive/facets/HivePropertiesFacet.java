/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.hive.client.Versions;
import java.util.Map;
import lombok.Getter;

@Getter
public class HivePropertiesFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("properties")
  private Map<String, Object> properties;

  public HivePropertiesFacet(Map<String, Object> environmentDetails) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.properties = environmentDetails;
  }
}
